from ophyd import DeviceStatus, Component as Cpt
import itertools
from .tes import TESBase
from sst_base.detectors.mca import EpicsMCABase

class TESMCA(TESBase):
    mca = Cpt(EpicsMCABase, "XF:07ID-ES{UCAL:ROIS}:", name="mca")

    def get_plot_hints(self):
        return self.mca.get_plot_hints()

    def set_roi(self, label, llim, ulim, plot=True):
        self.mca.set_roi(label, llim, ulim, plot=plot)

    def clear_roi(self, label):
        self.mca.clear_roi(label)

    def clear_all_rois(self):
        self.mca.clear_all_rois()

    def set_exposure(self, exp_time):
        self.mca.set_exposure(exp_time)
        return super().set_exposure(exp_time)

    def trigger(self):
        sts1 = super().trigger()
        sts2 = self.mca.trigger()
        return sts1 & sts2
    
    def stage(self):
        if self.verbose:
            print("Staging TES")
        self._data_index = itertools.count()
        self._completion_status = DeviceStatus(self)

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
        return super().unstage()
