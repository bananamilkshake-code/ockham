#include "olap.h"

#include "mysql_connection_details.h"

#include <assert.h>
#include <sstream>
#include <random>
#include <unordered_set>

const QStringList OLAP::DETALIZATION[] =
{
	{"Year", "Month"},
	{"City", "Supplier"},
	{"HTP", "Detail"},
};

const std::vector<std::vector<std::string>> OLAP::ROW_NAMES =
{
	{			// TIME
		"YEAR(sp.order_date)",
		"DATE_FORMAT(sp.order_date, \"%M, %Y\")"
	},
	{"s.city", "s.name"},	// PLACE
	{"p.htp", "p.name"}	// DETAIL
};

std::string sum(const std::string &s1, const std::string &s2)
{
	auto size = std::max(s1.length(), s2.length());
	std::string result;

	for (uint8_t i = 0; i < size; i++)
	{
		char c1 = (s1.length() > i) ? s1[i] : 0;
		char c2 = (s2.length() > i) ? s2[i] : 0;

		result += (c1 + c2) / 256;
	}

	return result;
}

std::string& operator/=(std::string &s1, const float denominator)
{
	for (uint8_t i = 0; i < s1.length(); i++)
		s1[i] /= denominator;

	return s1;
}

const OLAP::Shipment& OLAP::Shipment::operator += (const Shipment &other)
{
	this->weight += other.weight;
	this->detail_weight += other.detail_weight;
	this->price += other.price;
	this->detail_price += other.detail_price;
	this->quantity += other.quantity;
	this->detail_name = sum(this->detail_name, other.detail_name);
	this->city = sum(this->city, other.city);
	this->htp += other.htp;

	return *this;
}

const OLAP::Shipment OLAP::Shipment::operator / (const float denominator)
{
	Shipment result = *this;

	result.weight /= denominator;
	result.detail_weight /= denominator;
	result.price /= denominator;
	result.detail_price /= denominator;
	result.quantity /= denominator;
	result.detail_name /= denominator;
	result.city /= denominator;
	result.htp /= denominator;

	return result;
}

bool OLAP::Shipment::operator<(const Shipment &other) const
{
	return this->hash() < other.hash();
}

bool OLAP::Shipment::operator==(const Shipment &other) const
{
	return abs(this->weight - other.weight) < 0.0001
	&& abs(this->detail_weight - other.detail_weight) < 0.0001
	&& abs(this->price - other.price) < 0.0001
	&& abs(this->detail_price - other.detail_price ) < 0.0001
	&& this->quantity == other.quantity
	&& this->detail_name == other.detail_name
	&& this->city == other.city
	&& this->htp == other.htp;
}

float OLAP::Shipment::hash() const
{
	return this->weight + this->detail_weight +
		this->price + this->detail_price +
		this->quantity + std::hash<std::string>()(this->detail_name) +
		std::hash<std::string>()(this->city) + this->htp;
}

void OLAP::Cluster::recalc_position()
{
	auto sum = Shipment();
	for (auto element : this->elements)
		sum += element;

	this->m = sum / this->elements.size();
}

const std::string OLAP::ALL = "NULL";

float OLAP::distance(const Shipment &s1, const Shipment &s2)
{
	auto d = 0.0;

	d += abs(s1.weight - s2.weight);
	d += abs(s1.detail_weight - s2.detail_weight);
	d += abs(s1.price - s2.price);
	d += abs(s1.detail_price - s2.detail_price);
	d += abs(s1.quantity - s2.quantity);
	d += abs(std::hash<std::string>()(s1.detail_name) - std::hash<std::string>()(s2.detail_name));
	d += abs(std::hash<std::string>()(s1.city) - std::hash<std::string>()(s2.city));
	d += abs(s1.htp - s2.htp);

	return d;
}

OLAP::OLAP()
{
	this->connection = mysql_init(nullptr);
	bool connection_succeed = mysql_real_connect(this->connection, DB_HOST, DB_USER, DB_PASSWORD, DB_TABLE, 0, nullptr, 0);
	assert(connection_succeed);

	mysql_set_character_set(this->connection, "utf8");

	this->fill_values();
}

OLAP::~OLAP()
{
	mysql_close(this->connection);
}

OLAP::cube_t OLAP::calculate(OLAP::Type dim_1, uint8_t detalisation_1, OLAP::Type dim_2, uint8_t detalisation_2, OLAP::Type dim_3, uint8_t detalisation_3, std::string dim_z_value)
{
	std::string row_name_1 = ROW_NAMES[dim_1][detalisation_1];
	std::string row_name_2 = ROW_NAMES[dim_2][detalisation_2];
	std::string row_name_3 = ROW_NAMES[dim_3][detalisation_3];

	std::stringstream cube_query;
	cube_query << "SELECT " <<
		row_name_1 << " AS dim_1," <<
		row_name_2 << " AS dim_2," <<
		row_name_3 << " AS dim_3, " <<
		"SUM(sp.price) "
		"FROM shipments sp "
		"INNER JOIN suppliers s ON s.id = sp.sid "
		"INNER JOIN parts p ON p.id = sp.pid "
		"WHERE " << row_name_3 << " = \"" << dim_z_value << "\" "
		"GROUP BY dim_1, dim_2, dim_3 WITH ROLLUP";

	if (mysql_query(this->connection, cube_query.str().c_str()))
		return cube_t();

	return this->convert_result(mysql_use_result(this->connection));
}

const QStringList& OLAP::get_values_list(OLAP::Type dimension, uint8_t detalisation)
{
	return this->lists[dimension][detalisation];
}

void OLAP::classify(std::vector<std::string> &low_risk, std::vector<std::string> &middle_risk, std::vector<std::string> &high_risk) const
{
	std::string query = "SELECT risk, name FROM suppliers WHERE risk IN (1, 2, 3) ORDER BY name";
	if (mysql_query(this->connection, query.c_str()))
		return;

	auto result = mysql_use_result(this->connection);
	if (!result)
		return;

	MYSQL_ROW row;
	while (row = mysql_fetch_row(result))
	{
		std::string risk = row[0];
		auto supplier = row[1];

		if (risk == "1")
			low_risk.push_back(supplier);
		else if (risk == "2")
			middle_risk.push_back(supplier);
		else if (risk == "3")
			high_risk.push_back(supplier);
	}
}

void OLAP::clasterize(std::string date_from, std::string date_to) const
{
	std::string query ="SELECT "
			"sp.weight AS weight, "
			"sp.price AS price,"
			"sp.qty AS quantity,"
			"p.name AS part_name,"
			"s.city AS city, "
			"p.weight AS part_weight,"
			"p.HTP AS htp,"
			"sp.part_price AS part_price "
		"FROM (SELECT * FROM shipments WHERE order_date BETWEEN \"" + date_from + "\" AND \"" + date_to + "\") sp "
		"LEFT JOIN parts p ON sp.pid = p.id "
		"LEFT JOIN suppliers s ON sp.sid = s.id "
		"WHERE p.name IS NOT NULL";

	if (mysql_query(this->connection, query.c_str()))
		return;

	auto result = mysql_use_result(this->connection);
	if (!result)
		return;

	shipments_t shipments;
	MYSQL_ROW row;
	while (row = mysql_fetch_row(result))
	{
		Shipment shipment;

		shipment.weight = atof(row[0]);
		shipment.price = atof(row[1]);
		shipment.quantity = atoi(row[2]);
		shipment.detail_name = row[3];
		shipment.city = row[4];
		shipment.detail_weight = atof(row[5]);
		shipment.htp = atoi(row[6]);
		shipment.detail_price = atof(row[7]);

		shipments.insert(shipment);
	}

	this->k_means(shipments);
}

OLAP::cube_t OLAP::convert_result(MYSQL_RES *result)
{
	if (!result)
		return cube_t();

	cube_t cube;

	MYSQL_ROW row;
	while (row = mysql_fetch_row(result))
	{
		std::string dim_1(row[0] ? std::string(row[0]) : "NULL");
		std::string dim_2(row[1] ? std::string(row[1]) : "NULL");

		auto value = row[3];

		cube[dim_1][dim_2] = value;
	}

	return cube;
}

static constexpr uint8_t CLUSTERS_COUNT = 3;


namespace std {
	template <>
	struct hash<OLAP::Shipment> {
		size_t operator() (const OLAP::Shipment &s) const { return s.hash(); }
	};
}

void OLAP::k_means(const shipments_t &shipments) const
{
	if (shipments.empty())
		return;

	clusters_t clusters;

	// Initialize clusters: choose random element to be centers.
	std::unordered_set<Shipment> used_elements;
	for (uint8_t i = 0; i < CLUSTERS_COUNT; i++)
	{
		shipments_t::iterator element;
		do {
			element = shipments.begin();
			auto random_index = rand() % shipments.size();
			std::advance(element, random_index);
		} while(used_elements.count(*element));

		clusters.push_back(Cluster(*element));
		used_elements.insert(*element);
	}

	// Perform cluster optimisation untill convergence
	clusters_t old_clusters;
	while (true)
	{
		old_clusters = clusters;

		// Clear all elements of cluster
		for (auto cluster : clusters)
			cluster.elements.clear();

		// Attribute the closest cluster to each data point
		for (auto element : shipments)
		{
			Cluster *cluster = nullptr;
			for (auto &current_cluster : clusters)
			{
				auto current_distance = OLAP::distance(element, current_cluster.center());
				if (cluster == nullptr)
				{
					cluster = &current_cluster;
					continue;
				}

				auto distance = OLAP::distance(element, cluster->center());
				if (current_distance > distance)
					continue;

				cluster = &current_cluster;
			}
			cluster->elements.insert(element);
		}

		// Set the position of each cluster to the mean of all data points belonging to that cluster
		for (auto cluster : clusters)
			cluster.recalc_position();

		if (old_clusters == clusters)
			break;
	}
}

void OLAP::fill_values()
{
	static const std::vector<std::string> TABLE_NAMES =
	{
		"shipments sp",
		"suppliers s",
		"parts p"
	};

	this->lists.clear();

	for (uint8_t type = 0; type < ROW_NAMES.size(); type++)
	{
		this->lists.push_back(std::vector<QStringList>());
		for (uint8_t detalisation = 0; detalisation < ROW_NAMES[type].size(); detalisation++)
		{
			std::stringstream query;
			query << "SELECT " << ROW_NAMES[type][detalisation] << " AS param FROM " << TABLE_NAMES[type] << " GROUP BY param";

			mysql_query(this->connection, query.str().c_str());
			auto res = mysql_store_result(this->connection);

			QStringList values;
			if (res)
			{
				MYSQL_ROW row;
				while (row = mysql_fetch_row(res))
					values.append(row[0]);
			}

			this->lists[type].push_back(values);
		}
	}
}
