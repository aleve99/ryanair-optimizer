from time import perf_counter_ns

class Timer:
    def __init__(self, start: bool = False) -> None:
        self._t_start = None
        self._t_stop = None

        if start:
            self.start()

    def start(self):
        self._t_start = perf_counter_ns()
    
    def stop(self):
        self._t_stop = perf_counter_ns()
    
    @property
    def seconds_elapsed(self, decimals: int = 5): 
        return round(1e-9 * (self._t_stop - self._t_start), decimals)