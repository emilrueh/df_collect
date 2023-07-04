import winsound


def oh_oh():
    winsound.Beep(frequency=900, duration=300)
    winsound.Beep(frequency=600, duration=200)


def crit_error():
    winsound.Beep(frequency=600, duration=600)
    winsound.Beep(frequency=400, duration=800)
    winsound.Beep(frequency=200, duration=1000)


def success():
    winsound.Beep(frequency=1000, duration=200)
    winsound.Beep(frequency=1000, duration=100)
    winsound.Beep(frequency=1000, duration=200)
    winsound.Beep(frequency=1400, duration=200)
    winsound.Beep(frequency=1200, duration=400)
