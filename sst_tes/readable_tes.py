from ophyd import DeviceStatus, Device, Component, Kind
from ophyd.signal import AttributeSignal, Signal
import time as ttime
import threading
from queue import Queue, Empty
from collections import OrderedDict, deque
import itertools
from os.path import join, relpath
from .tes_signals import *
from .rpc import RPCInterface
from event_model import compose_resource
from .tes import TESBase, raiseOnFailure
from sst_base.detectors.mca import EpicsMCABase

class TES(TESBase):
    _fast_read = True

    @property
    def hints(self):
        return self._hints

    def describe(self):
        d = super().describe()
        if self.write_off:
            for k in self.rois:
                key = self.name + "_" + k
                d[key] = {"dtype": "number", "shape": [], "source": key,
                          "llim": self.rois[k][0], "ulim": self.rois[k][1]}
        return d

    def read(self):
        d = super().read()
        if self.write_off:
            msg = self.rpc.roi_get_counts(fast=self._fast_read)
            if msg['success']:
                rois = msg['response']
                for k in self.rois:
                    key = self.name + "_" + k
                    val = rois[k]
                    d[key] = {"value": val, "timestamp": self.last_time}
            else:
                for k in self.rois:
                    key = self.name + "_" + k
                    d[key] = {"value": 0, "timestamp": self.last_time}
        return d

    def stage(self):
        if self.verbose:
            print("Staging TES")
        self._data_index = itertools.count()
        self._completion_status = DeviceStatus(self)
        self._external_devices = [dev for _, dev in self._get_components_of_kind(Kind.normal)
                                  if hasattr(dev, 'collect_asset_docs')]

        if self.file_mode == "start_stop":
            self._file_start()

        if self.state.get() == "no_file":
            self._file_start()
            # raise ValueError(f"{self.name} has no file open, cannot stage.")

        if self.cal_flag.get():
            self._calibration_start()
        else:
            self._scan_start()

        return super().stage()

    def unstage(self):
        if self.verbose: print("Complete acquisition of TES")
        self._scan_end()
        if self.file_mode == "start_stop":
            self._file_end()
        self._log = {}
        self._data_index = None
        self._external_devices = None
        return super().unstage()


class SIMTES(TES):
    def __init__(self, name, motor, motor_field, *args, **kwargs):
        super().__init__(name=name, **kwargs)
        self._motor = motor
        self._motor_field = motor_field

    def _acquire(self, status, i):
        t1 = self._motor.read()[self._motor_field]['value']
        t2 = t1 + self.acquire_time.get()
        self.rpc.scan_point_start(i, t1)
        ttime.sleep(self.acquire_time.get())
        self.rpc.scan_point_end(t2)
        if self._save_roi:
            self.rpc.roi_save_counts()
        status.set_finished()
        return
