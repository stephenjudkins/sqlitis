CREATE TABLE person(
    id INT PRIMARY KEY,
    name text NOT NULL,
    age INT NOT NULL
);

CREATE TABLE pet(
    id INT PRIMARY KEY,
    name text NOT NULL,
    person_id INT NOT NULL REFERENCES person
);

INSERT INTO person VALUES (1, 'Charlie Brown', 8), (2, 'Calvin', 6);
INSERT INTO pet VALUES (1, 'Snoopy', 1), (2, 'Hobbes', 2);

