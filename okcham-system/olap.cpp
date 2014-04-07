#include "olap.h"

#include "mysql_connection_details.h"

#include <assert.h>
#include <sstream>
#include <random>
#include <unordered_set>
/*
OLAP::position_t operator+(const OLAP::position_t &p1, const OLAP::position_t &p2)
{
	return p1;
}
*/
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

const std::string OLAP::ALL = "NULL";

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

static float distance(const OLAP::position_t &s1, const OLAP::position_t &s2)
{
	return 0.0;
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
			"sp.part_price AS part_price"
		"FROM (SELECT * FROM shipments WHERE order_date BETWEEN " + date_from + " AND " + date_to + " ) sp"
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

size_t hash_shipment(const OLAP::Shipment &s) { return (size_t)s.position;}

namespace std {
	template <>
	struct hash<OLAP::Shipment> {
		size_t operator() (const OLAP::Shipment &s) const { return hash_shipment(s); }
	};
}

void OLAP::k_means(const shipments_t &shipments) const
{
	clusters_t clusters;

	// Initialize clusters: choose random element to be centers.
	std::unordered_set<Shipment> used_elements;
	for (uint8_t i = 0; i < CLUSTERS_COUNT; i++)
	{
		auto element = shipments.begin();
		do {
			auto random_index = 0;
			std::advance(element, random_index);
		} while(used_elements.count(*element));

		clusters.push_back(Cluster(*element));
		used_elements.insert(*element);
	}

	// Perform cluster optimisation untill convergence
	clusters_t old_clasters = clusters;
	do {
		// Clear all elements of cluster
		for (auto cluster : clusters)
			cluster.elements.clear();

		// Attribute the closest cluster to each data point
		for (auto element : shipments)
		{
			Cluster *cluster = nullptr;
			for (auto current_cluster : clusters)
			{
				auto distance = ::distance(element.position, cluster->center());
				auto current_distance = ::distance(element.position, current_cluster.center());
				if (cluster == nullptr && current_distance > distance)
					continue;

				cluster = &current_cluster;
			}
			cluster->elements.insert(element);
		}

		// Set the position of each cluster to the mean of all data points belonging to that cluster
		for (auto cluster : clusters)
			cluster.recalc_position();
	} while (old_clasters != clusters);
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
