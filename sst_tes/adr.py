from ophyd import Device, Component, Kind
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
        
