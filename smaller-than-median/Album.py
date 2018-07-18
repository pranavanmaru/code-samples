class Track:
    def __init__(self, title, duration):
        self.title = title
        self.duration = duration


class Album:
    def __init__(self, title, artist, year):
        self.title = title
        self.artist = artist
        self.year = year
        self.track_list = []

    def get_durations(self):
        durations = map(lambda x: x.duration, self.track_list)

        return list(durations)

    def get_lower_half(self, median):
        track_list = []
        album = Album(self.title, self.artist, self.year)

        for track in self.track_list:
            if track.duration <= median:
                track_list.append(track)

        track_list.sort(key=lambda x: x.duration, reverse=False)
        album.track_list = track_list
        return album