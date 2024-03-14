# Bootcamp-Final-Project
This is the final project for Digital Skola Bootcamp for Data Engineer 

Final Project ini membuat ETL untuk

1. Ekstrak data covid_jabar dari API dan menyimpannya ke database di mysql.
2. Menarik data covid_jabar dari mysql dan mentranformasikannya menjadi dimension table (data provinsi, data kabupaten, dan data kasus).
3. Membuat 2 fact table yaitu data covid harian provinsi, dan data covid harian kabupaten.
4. Menyimpan dimension table dan fact table ke dalam data warehouse di Postgres.

Final Project Flow

![img1](dags/img/Picture1.png)

data covid_jabar
![img2](dags/img/Screenshot%20(155).png)

data provinsi
![img2](dags/img/Screenshot%20(149).png)

data kabupaten
![img2](dags/img/Screenshot%20(150).png)

data kasus
![img2](dags/img/Screenshot%20(151).png)

data covid harian provinsi
![img2](dags/img/Screenshot%20(152).png)

data covid harian kabupaten
![img2](dags/img/Screenshot%20(153).png)

