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
    filename = Component(RPCSignal, method="filename", kind=Kind.config)
    calibration = Component(RPCSignal, method='calibration_state', kind=Kind.config)
    state = Component(RPCSignal, method='state', kind=Kind.config)
    scan_num = Component(RPCSignal, method='scan_num', kind=Kind.config)
    scan_str = Component(RPCSignal, method='scan_str', kind=Kind.config)

    def __init__(self, name, *args, verbose=False, path=None, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        self._hints = {'fields': [f'{name}_tfy']}
        self._log = {}
        self._completion_status = None
        self._save_roi = False
        self.verbose = verbose
        self.file_mode = "continuous"  # Or "start_stop"
        self.write_ljh = True
        self.write_off = True
        self.rois = {"tfy": (0, 1200)}
        self.last_time = 0
        self.last_noise_file = None
        self.last_projector_file = None
        self.path = path
        self.setFilenamePattern = True
        self.scanexfiltrator = None
        self._commStatus = "Disconnected"
        self._connected = False
        self._rsync_on_file_end = False
        self._rsync_on_scan_end = False

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
        return self.rpc.calibration_start(var_name, var_unit, scan_num,
                                          sample_id, sample_name, routine)

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

        last_time = self.rpc.scan_point_start(val)['response']
        self.last_time = float(last_time)
        ttime.sleep(self.acquire_time.get())
        msg = self.rpc.scan_point_end()
        # self.last_time = ttime.time()
        status.set_finished()
        return msg

    @raiseOnFailure
    def take_noise(self, path=None, time=4):
        self.rpc.set_noise_triggers()
        if path is None:
            path = self.path
        start_msg = self.rpc.file_start(path, write_ljh=True, write_off=False,
                                        setFilenamePattern=self.setFilenamePattern)
        self.last_noise_file = start_msg['response']
        ttime.sleep(time)
        msg = self._file_end()
        self.rpc.set_pulse_triggers()
        return msg

    @raiseOnFailure
    def take_projectors(self, path=None, time=60):
        self.rpc.set_pulse_triggers()
        if path is None:
            path = self.path
        start_msg = self.rpc.file_start(path, write_ljh=True, write_off=False,
                                        setFilenamePattern=self.setFilenamePattern)
        self.last_projector_file = start_msg['response']
        ttime.sleep(time)
        msg = self._file_end()
        return msg

    @raiseOnFailure
    def make_projectors(self):
        msg = self.rpc.make_projectors(self.last_noise_file,
                                       self.last_projector_file)
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

    def describe(self):
        d = super().describe()
        if self.write_off:
            for k in self.rois:
                key = self.name + "_" + k
                d[key] = {"dtype": "number", "shape": [], "source": key,
                          "llim": self.rois[k][0], "ulim": self.rois[k][1]}
        return d

    @property
    def hints(self):
        return self._hints

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
