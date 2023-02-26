from ophyd import Device, Component, Kind, EpicsSignal, EpicsSignalRO
from .tes_signals import RPCSignalRO, RPCSignalPair
from .rpc import RPCInterface


class ADR(Device, RPCInterface):
    state = Component(RPCSignalRO, method='get_state_label')
    temperature_sp = Component(RPCSignalPair, get_method='get_temp_sp_k', set_method='set_temp_sp_k')
    t50mk = Component(RPCSignalRO, method='get_temp_k')
    t1k = Component(RPCSignalRO, method='get_alt_temp_k')
    heater = Component(RPCSignalRO, method='get_hout')
    rms_uk = Component(RPCSignalRO, method='get_temp_rms_uk')
    cycle_uid = Component(RPCSignalRO, method='get_cycle_uid')

    def start_cycle(self):
        response = self.rpc.sendrcv('start_mag_cycle')

class EPICS_ADR(Device):
    state = Component(EpicsSignalRO, ":STATE")
    t50mk = Component(EpicsSignalRO, ":TEMP", kind='hinted')
    t50mk_sp = Component(EpicsSignal, ":TEMP_SP_RB", write_pv=":TEMP_SP")
    t1k = Component(EpicsSignalRO, ":ALT_TEMP")
    heater = Component(EpicsSignalRO, ":HEATER_OUT", kind='hinted')
    rms_uk = Component(EpicsSignalRO, ":TEMP_RMS_UK")
    cycle_uid = Component(EpicsSignalRO, ":CYCLE_UID")
    pause_pv = Component(EpicsSignal, ":PAUSE", kind='omitted')
    cycle_pv = Component(EpicsSignal, ":START_CYCLE", kind="omitted")

    def pause_pid(self):
        self.pause_pv.set(1)

    def unpause_pid(self):
        self.pause_pv.set(0)

    def start_cycle(self):
        self.cycle_pv.set(1)
