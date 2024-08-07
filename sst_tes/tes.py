from ophyd import DeviceStatus, Device, Component, Kind
from ophyd.signal import AttributeSignal, Signal
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
        if not response['success']:
            raise TESException(f"RPC failed with message {response['response']}")
        return response
    return _inner


class TESBase(Device, RPCInterface):
    _cal_flag = False
    _acquire_time = 1

    cal_flag = Component(AttributeSignal, '_cal_flag', kind=Kind.config)
    acquire_time = Component(AttributeSignal, '_acquire_time', kind=Kind.config)
    commStatus = Component(AttributeSignal, '_commStatus', kind=Kind.config)
    connected = Component(AttributeSignal, '_connected', kind=Kind.config)
    noise_uid = Component(AttributeSignal, '_last_noise_uid', kind=Kind.config)
    projector_uid = Component(AttributeSignal, '_last_projector_uid', kind=Kind.config)
    filename = Component(RPCSignal, method="filename", kind=Kind.config)
    state = Component(RPCSignal, method='state', kind=Kind.config)
    scan_num = Component(RPCSignal, method='scan_num', kind=Kind.config)
    scan_str = Component(RPCSignal, method='scan_str', kind=Kind.config)
    scan_point_start = Component(AttributeSignal, '_scan_point_start', kind=Kind.normal)
    scan_point_end = Component(AttributeSignal, '_scan_point_end', kind=Kind.normal)

    def __init__(self, prefix, *, name, verbose=False, path=None, setFilenamePattern=False, **kwargs):
        super().__init__(prefix, name=name, **kwargs)
        self._hints = {'fields': [f'{name}_tfy']}
        self._log = {}
        self._completion_status = None
        self._save_roi = False
        self.verbose = verbose
        self.file_mode = "continuous"  # Or "start_stop"
        self.write_ljh = False
        self.write_off = True
        self.rois = {"tfy": (0, 1200)}
        self.last_time = 0
        self._last_noise_file = None
        self._last_projector_file = None
        self._last_noise_uid = ""
        self._last_projector_uid = ""
        self.path = path
        self.setFilenamePattern = setFilenamePattern
        self.scanexfiltrator = None
        self._commStatus = "Disconnected"
        self._connected = False
        self._scan_point_start = 0
        self._scan_point_end = 0
        self._rsync_on_file_end = True
        self._rsync_on_scan_end = True

    def _commCheck(self):
        try:
            msg = self.rpc.commCheck()
        except RPCException:
            msg = {'success': False, 'response': 'Disconnected'}
        self._connected = msg['success']
        self._commStatus = msg['response']

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
            msg = self.rpc.file_start(path, write_ljh=self.write_ljh, write_off=self.write_off, setFilenamePattern=self.setFilenamePattern)
            return msg
        else:
            print("TES already has file open, not forcing!")
            return {"success": True, "response": "File already open"}

    @raiseOnFailure
    def _file_end(self):
        return self.rpc.file_end(_try_rsync_data=self._rsync_on_file_end)

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
        sample_name = scaninfo.get("sample_name", 'null')
        # start_energy = scaninfo.get("start_energy", -1)
        routine = 'simulated_source'
        if self.verbose:
            print(f"start calibration scan {scan_num}")
        return self.rpc.calibration_start(var_name, var_unit,
                                          sample_id, sample_name)

    @raiseOnFailure
    def _scan_start(self):
        if self.scanexfiltrator is not None:
            scaninfo = self.scanexfiltrator.get_scan_start_info()
        else:
            scaninfo = {}
        var_name = scaninfo.get("motor", "unnamed_motor")
        var_unit = scaninfo.get("motor_unit", "index")
        sample_id = scaninfo.get("sample_id", -1)
        sample_name = scaninfo.get("sample_name", 'null')
        start_energy = scaninfo.get("start_energy", -1)
        if self.verbose:
            print(f"start scan {scan_num}")
        msg = self.rpc.scan_start(var_name, var_unit, sample_id, sample_name, extra={"start_energy": start_energy})
        return msg

    @raiseOnFailure
    def _scan_end(self):
        msg = self.rpc.scan_end(_try_post_processing=False, _try_rsync_data=self._rsync_on_scan_end)
        self.scanexfiltrator = None
        return msg

    def _acquire(self, status, i):
        # t1 = ttime.time()
        # t2 = t1 + self.acquire_time.get()
        if self.scanexfiltrator is not None:
            val = self.scanexfiltrator.get_scan_point_info()
        else:
            val = i

        start_time = self.rpc.scan_point_start(val)['response']
        self._scan_point_start = float(start_time)
        self.last_time = float(start_time)
        ttime.sleep(self.acquire_time.get())
        msg = self.rpc.scan_point_end()
        end_time = float(msg['response'])
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
        start_msg = self.rpc.file_start(path, write_ljh=True, write_off=False,
                                        setFilenamePattern=self.setFilenamePattern)
        self._last_noise_file = start_msg['response']
        return start_msg
        """
        ttime.sleep(time)
        msg = self._file_end()
        self.rpc.set_pulse_triggers()
        return msg
        """
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
        start_msg = self.rpc.file_start(path, write_ljh=True, write_off=False,
                                        setFilenamePattern=self.setFilenamePattern)
        self._last_projector_file = start_msg['response']
        return start_msg
        """
        ttime.sleep(time)
        msg = self._file_end()
        return msg
        """
        
    @raiseOnFailure
    def make_projectors(self):
        msg = self.rpc.make_projectors(self._last_noise_file,
                                       self._last_projector_file)
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
