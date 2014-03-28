CREATE TABLE suppliers (
	id INT (10) AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(20) NOT NULL,
	risk TINYINT(2) CHECK(risk IN (1, 2, 3))
);

CREATE TABLE parts (
	id INT (10) AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(20) NOT NULL,
	HTP TINYINT(1) NOT NULL,
	Weight FLOAT NOT NULL CHECK (Weight > 0),
	UNIQUE(name)
);

CREATE TABLE shipments (
	id INT(10) AUTO_INCREMENT PRIMARY KEY,
	sid INT(10) NOT NULL,
	pid INT(10) NOT NULL,
	qty SMALLINT NOT NULL CHECK (qty > 0),
	price SMALLINT NOT NULL CHECK (price > 0),
	weight SMALLINT NOT NULL CHECK (weight > 0 AND weight <= 1500),
	order_date INT(10) NOT NULL,
	period TINYINT(3) NOT NULL CHECK(period > 0),
	ship_date INT(10) NOT NULL CHECK(ship_date <= order_date),

	FOREIGN KEY (sid) REFERENCES suppliers(id),
	FOREIGN KEY (pid) REFERENCES parts(id)
);

CREATE TABLE addresses (
	id INT(10) AUTO_INCREMENT PRIMARY KEY,
	sid INT(10) NOT NULL,
	city VARCHAR(20) NOT NULL,
	address VARCHAR(20) NOT NULL,

	UNIQUE(sid, city, address),

	FOREIGN KEY (sid) REFERENCES suppliers(id)
);

CREATE TABLE updates (
	time INT(10) NOT NULL,
	affiliate_id SMALLINT(5) NOT NULL,
	s INT(10) NOT NULL,
	p INT(10) NOT NULL,
	sp INT(10) NOT NULL
);

CREATE INDEX update_time ON updates(time);	