from ophyd import (
    DeviceStatus,
    Device,
    Component as Cpt,
    Kind,
    FormattedComponent as FCpt,
)
from ophyd.signal import AttributeSignal, Signal, EpicsSignal
import time as ttime
import threading
from queue import Queue, Empty
from collections import OrderedDict, deque
import itertools
from os.path import join, relpath
from .tes_signals import *
from .rpc import RPCInterface, RPCException
from functools import wraps


class TESException(Exception):
    pass


def raiseOnFailure(f):
    def _inner(*args, **kwargs):
        response = f(*args, **kwargs)
        if not response["success"]:
            raise TESException(f"RPC failed with message {response['response']}")
        return response

    return _inner


class TESBase(Device, RPCInterface):
    _cal_flag = False
    _acquire_time = 1
    """
    Caproto concept:
    We talk directly to TES Server
    TES Server writes PVs to Caproto MCA via py EPICS
    In this concept we require Caproto MCA or no data at all
    """
    cal_flag = FCpt(AttributeSignal, "_cal_flag", kind=Kind.config)  # PCASpy
    status = Cpt(EpicsSignal, "STATUS", kind=Kind.config)
    acquire_time = FCpt(AttributeSignal, "_acquire_time", kind=Kind.config)  # TESMCA?
    commStatus = FCpt(AttributeSignal, "_commStatus", kind=Kind.config)  # PCASpy
    connected = Cpt(EpicsSignal, "CONNECTED", kind=Kind.config)
    running = Cpt(EpicsSignal, "RUNNING", kind=Kind.config)
    noise_uid = Cpt(EpicsSignal, "NOISE_UID", string=True, kind=Kind.config)
    projector_uid = Cpt(EpicsSignal, "PROJECTOR_UID", string=True, kind=Kind.config)
    noise_filename = Cpt(EpicsSignal, "NOISE_FILE", string=True, kind=Kind.config)
    projector_filename = Cpt(
        EpicsSignal, "PROJECTOR_FILE", string=True, kind=Kind.config
    )
    projectors_loaded = Cpt(EpicsSignal, "PROJECTORS", kind=Kind.config)
    calibration_uid = Cpt(EpicsSignal, "CALIBRATION_UID", string=True, kind=Kind.config)
    filename = Cpt(EpicsSignal, "FILENAME", string=True, kind=Kind.config)
    state = Cpt(EpicsSignal, "STATE", string=True, kind=Kind.config)
    scan_num = Cpt(EpicsSignal, "SCAN_NUM", kind=Kind.config)
    scan_str = Cpt(EpicsSignal, "SCAN_STR", string=True, kind=Kind.config)
    scan_point_start = FCpt(
        AttributeSignal, "_scan_point_start", kind=Kind.normal, add_prefix=()
    )
    scan_point_end = FCpt(
        AttributeSignal, "_scan_point_end", kind=Kind.normal, add_prefix=()
    )
    rsync_on_file_end = Cpt(EpicsSignal, "RSYNC_ON_FILE_END", kind=Kind.config)
    rsync_on_scan_end = Cpt(EpicsSignal, "RSYNC_ON_SCAN_END", kind=Kind.config)
    write_ljh = Cpt(EpicsSignal, "WRITE_LJH", kind=Kind.config)
    write_off = Cpt(EpicsSignal, "WRITE_OFF", kind=Kind.config)

    def __init__(
        self,
        prefix,
        *,
        name,
        verbose=False,
        path=None,
        setFilenamePattern=False,
        **kwargs,
    ):
        super().__init__(prefix, name=name, **kwargs)
        self._hints = {"fields": [f"{name}_tfy"]}
        self._log = {}
        self._completion_status = None
        self._save_roi = False
        self.verbose = verbose
        self.file_mode = "continuous"  # Or "start_stop"
        self.rois = {"tfy": (0, 1200)}
        self.last_time = 0
        self._last_noise_file = None
        self._last_projector_file = None
        self.path = path
        self.setFilenamePattern = setFilenamePattern
        self.scanexfiltrator = None
        self._commStatus = "Disconnected"
        self._connected = False
        self._scan_point_start = 0
        self._scan_point_end = 0

    def _commCheck(self):
        try:
            msg = self.rpc.commCheck()
        except RPCException:
            msg = {"success": False, "response": "Disconnected"}
        self._connected = msg["success"]
        self._commStatus = msg["response"]

    @raiseOnFailure
    def _file_start(self, path=None, force=False):
        """
        Starts file writing. If self.setFilenamePattern,
        path should be something that can be formatted by datetime.strftime,
        i.e., /nsls2/data/sst1/legacy/ucal/raw/%Y/%m/%d
        This should certainly be the default, and file_start should not generally be called
        with arguments
        """

        if path is None:
            path = self.path
        if self.state.get() == "no_file" or force:
            msg = self.rpc.file_start(
                path,
                setFilenamePattern=self.setFilenamePattern,
            )
            return msg
        else:
            print("TES already has file open, not forcing!")
            return {"success": True, "response": "File already open"}

    @raiseOnFailure
    def _file_end(self):
        return self.rpc.file_end()

    @raiseOnFailure
    def _calibration_start(self):
        if self.scanexfiltrator is not None:
            scaninfo = self.scanexfiltrator.get_scan_start_info()
        else:
            scaninfo = {}
        var_name = scaninfo.get("motor", "unnamed_motor")
        var_unit = scaninfo.get("motor_unit", "index")
        scan_num = self.scan_num.get()
        sample_id = scaninfo.get("sample_id", -1)
        sample_name = scaninfo.get("sample_name", "null")
        # start_energy = scaninfo.get("start_energy", -1)
        routine = "simulated_source"
        if self.verbose:
            print(f"start calibration scan {scan_num}")
        return self.rpc.calibration_start(var_name, var_unit, sample_id, sample_name)

    @raiseOnFailure
    def _scan_start(self):
        if self.scanexfiltrator is not None:
            scaninfo = self.scanexfiltrator.get_scan_start_info()
        else:
            scaninfo = {}
        var_name = scaninfo.get("motor", "unnamed_motor")
        var_unit = scaninfo.get("motor_unit", "index")
        sample_id = scaninfo.get("sample_id", -1)
        sample_name = scaninfo.get("sample_name", "null")
        start_energy = scaninfo.get("start_energy", -1)

        msg = self.rpc.scan_start(
            var_name,
            var_unit,
            sample_id,
            sample_name,
            extra={"start_energy": start_energy},
        )
        return msg

    @raiseOnFailure
    def _scan_end(self):
        msg = self.rpc.scan_end(_try_post_processing=False)
        self.scanexfiltrator = None
        return msg

    @property
    def path(self):
        if hasattr(self, "_dynamic_path"):
            path = self._dynamic_path()
        else:
            path = self._path
        return path

    @path.setter
    def path(self, path):
        self._path = path
        
    def _acquire(self, status, i):
        # t1 = ttime.time()
        # t2 = t1 + self.acquire_time.get()
        if self.scanexfiltrator is not None:
            val = self.scanexfiltrator.get_scan_point_info()
        else:
            val = i

        start_time = self.rpc.scan_point_start(val)["response"]
        self._scan_point_start = float(start_time)
        self.last_time = float(start_time)
        ttime.sleep(self.acquire_time.get())
        msg = self.rpc.scan_point_end()
        end_time = float(msg["response"])
        self._scan_point_end = end_time
        # self.last_time = ttime.time()
        status.set_finished()
        return msg

    @raiseOnFailure
    def take_noise(self, path=None):
        self._set_noise_triggers()
        self.set_exposure(1.0)
        if path is None:
            path = self.path
        start_msg = self.rpc.file_start(
            path,
            write_ljh=True,
            write_off=False,
            setFilenamePattern=self.setFilenamePattern,
        )
        noise_file = start_msg["response"]
        self.noise_filename.set(noise_file).wait()
        self._last_noise_file = noise_file
        return start_msg

    @raiseOnFailure
    def _set_pulse_triggers(self):
        msg = self.rpc.set_pulse_triggers()
        return msg

    @raiseOnFailure
    def _set_noise_triggers(self):
        msg = self.rpc.set_noise_triggers()
        return msg

    @raiseOnFailure
    def take_projectors(self, path=None):
        self._set_pulse_triggers()
        self.set_exposure(1.0)
        if path is None:
            path = self.path
        start_msg = self.rpc.file_start(
            path,
            write_ljh=True,
            write_off=False,
            setFilenamePattern=self.setFilenamePattern,
        )
        projector_file = start_msg["response"]
        self.projector_filename.set(projector_file).wait()
        self._last_projector_file = projector_file
        return start_msg

    @raiseOnFailure
    def make_projectors(self):
        noise_file = self.noise_filename.get()
        projector_file = self.projector_filename.get()
        msg = self.rpc.make_projectors(noise_file, projector_file)
        return msg

    @raiseOnFailure
    def set_projectors(self):
        msg = self.rpc.set_projectors()
        return msg

    def set_roi(self, label, llim, ulim):
        self.rois[label] = (llim, ulim)
        return self.rpc.roi_set({label: (llim, ulim)})

    def clear_roi(self, label):
        self.rois.pop(label)
        return self.rpc.roi_set({label: (None, None)})

    def trigger(self):
        if self.verbose:
            print("Triggering TES")
        status = DeviceStatus(self)
        i = next(self._data_index)
        threading.Thread(target=self._acquire, args=(status, i), daemon=True).start()
        return status

    def stop(self):
        if self._completion_status is not None:
            self._completion_status.set_finished()

    def set_exposure(self, exp_time):
        self.acquire_time.put(exp_time)
