CREATE TABLE suppliers (
	id INT (10) AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(20) NOT NULL,
	city VARCHAR(20) NOT NULL,
	address VARCHAR(20) NOT NULL,
	risk TINYINT(2) CHECK(risk IN (0, 1, 2, 3)),
	shipments_count INT(10) NOT NULL DEFAULT 0,
	shipments_delays INT(10) NOT NULL DEFAULT 0,
	details_count INT(10) NOT NULL DEFAULT 0,

	UNIQUE(name, city, address)
);

CREATE TABLE parts (
	id INT (10) AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(20) NOT NULL,
	HTP TINYINT(1) NOT NULL,
	weight FLOAT NOT NULL CHECK (Weight > 0),

	UNIQUE(name, weight)
);

CREATE TABLE shipments (
	sid INT(10) NOT NULL,
	pid INT(10) NOT NULL,
	qty SMALLINT NOT NULL CHECK (qty > 0),
	price SMALLINT NOT NULL CHECK (price > 0),
	weight SMALLINT NOT NULL CHECK (weight > 0 AND weight <= 1500),
	order_date DATE NOT NULL,
	period TINYINT(3) NOT NULL CHECK(period > 0),
	ship_date DATE NOT NULL CHECK(ship_date <= order_date),

	FOREIGN KEY (sid) REFERENCES suppliers(id),
	FOREIGN KEY (pid) REFERENCES parts(id)
);

CREATE TABLE updates (
	time INT(10) NOT NULL,
	affiliate_id SMALLINT(5) NOT NULL,
	s INT(10) NOT NULL,
	p INT(10) NOT NULL,
	sp INT(10) NOT NULL
);

CREATE INDEX update_time ON updates(time);	
