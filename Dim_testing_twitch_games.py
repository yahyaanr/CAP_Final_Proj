import csv

input_file = "/home/faisyadd/Twitchapi_gabungan/testing_streamers_30Mei_5.tsv"
output_file = "/home/faisyadd/Twitchapi_gabungan/dim_games.tsv"

# Buat set untuk menyimpan nilai unik dari games_id dan game_name
games_id_set = set()
game_name_set = set()

# Buka file input dan baca setiap baris
with open(input_file, 'r', newline='') as input_file:
    reader = csv.reader(input_file, delimiter='\t')
    next(reader)  # Skip header

    for row in reader:
        games_id = row[7]  # Kolom ke-7
        game_name = row[8]  # Kolom ke-8

        # Tambahkan nilai games_id dan game_name ke set
        games_id_set.add(games_id)
        game_name_set.add(game_name)

# Tulis nilai dari set ke dalam file output
with open(output_file, 'w', newline='') as output_file:
    writer = csv.writer(output_file, delimiter='\t')

    # Tulis header baru ke file output
    writer.writerow(['games_id', 'game_name'])

    # Tulis nilai dari set ke dalam file output
    for games_id, game_name in zip(games_id_set, game_name_set):
        writer.writerow([games_id, game_name])

print("Data telah berhasil disimpan dalam file games.tsv.")