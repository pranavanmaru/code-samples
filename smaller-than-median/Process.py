from Album import Album, Track
import functools as ft

album_list = []

# Helper Methods
def read_file(path):
    file = open(path, "r")
    return file.readlines()


def get_duration_seconds(min):
    tokens = min.split(':')
    minute = int(tokens[0])
    sec = int(tokens[1])

    return (minute * 60) + sec


def get_duration_min_sec(seconds):
    min = seconds // 60
    sec = seconds % 60

    return f"{min}:{sec}" if sec >= 10 else f"{min}:0{sec}"


def get_median(durations):
    durations.sort()
    index = len(durations)//2 + len(durations)%2
    median = durations[index] if len(durations) % 2 == 0 else (durations[index] + durations[index+1])/2

    return median


def print_lower_half(lower_half):
    count = 1
    total_duration = 0

    for album in lower_half:
        for track in album.track_list:
            print(f"{count}. {album.artist} - {album.title} - {album.year} - {track.title} - {get_duration_min_sec(track.duration)}")
            count += 1
            total_duration += track.duration

    print()
    print(f"Total Time: <{get_duration_min_sec(total_duration)}>")


# Initiates Data Structure that holds album and track information
def init_db():
    file = read_file("./Albums.txt")

    if not file:
        print("File Does Not Exist. Make Sure 'Albums.txt' is in the same folder")

    album = None

    for line in file:
        if line.strip() == '':
            continue
            
        elif line.__contains__('/') or line.strip() == '':
            tokens = line.split('/')
            artist = tokens[0].strip()
            title = tokens[1].strip()
            year = tokens[2].strip()
            album = Album(artist, title, year)
            album_list.append(album)
            
        elif album:
            tokens = line.split('-')
            title = tokens[0].strip()
            duration = get_duration_seconds(tokens[1].strip())
            track = Track(title, duration)
            album.track_list.append(track)


def init_process():
    init_db()
    duration_list = map(lambda x: x.get_durations(), album_list)
    durations = list(ft.reduce(lambda x, y: x+y, duration_list))
    median = get_median(durations)

    lower_half = list(map(lambda x: x.get_lower_half(median), album_list))
    print_lower_half(lower_half)
    print(f"Median: {get_duration_min_sec(median)}")


if __name__ == '__main__':
    init_process()