#ifndef OLAP_H
#define OLAP_H

#include <map>
#include <string>
#include <vector>
#include <set>
#include <mysql/mysql.h>

#include <QStringList>

class OLAP
{
	MYSQL *connection;

public:
	enum Type : uint8_t
	{
		TIME,
		PLACE,
		DETAIL
	};

	typedef float position_t;

	struct Shipment
	{
		float weight;
		float detail_weight;
		float price;
		float detail_price;
		uint64_t quantity;
		std::string detail_name;
		std::string city;
		bool htp;

		position_t position;

		bool operator<(const Shipment &other) const { return this->position < other.position; }
	};

	struct Cluster
	{
		Cluster(Shipment center): m(center.get_position()) {}

		shipments_t elements;

		void recalc_position()
		{
			auto sum = position_t();
			for (auto element : this->elements)
				sum += get_position();

			this->m = sum / this->elements.size();
		}

		bool operator==(const Cluster &other) const { return abs(this->m - other.m) < 0.0001; }

	private:
		position_t m;
	};

	typedef std::map<std::string, std::map<std::string, std::string>> cube_t;
	typedef std::set<Shipment> shipments_t;
	typedef std::set<Cluster> clusters_t;

	static const QStringList DETALIZATION[];
	static const std::vector<std::vector<std::string>> ROW_NAMES;

	static const std::string ALL;

	OLAP();
	~OLAP();

	cube_t calculate(Type dim_1, uint8_t detalization_1, Type dim_2, uint8_t detalization_2, Type dim_3, uint8_t detalization_3, std::string dim_z_value);

	void fill_values();

	const QStringList& get_values_list(Type dimension, uint8_t detalisation);
	void classify(std::vector<std::string> &low_risk, std::vector<std::string> &middle_risk, std::vector<std::string> &high_risk) const;
	void clasterize(std::string date_from, std::string date_to) const;

private:
	typedef std::vector<std::vector<QStringList>> values_lists_t;
	values_lists_t lists;

	cube_t convert_result(MYSQL_RES *result);

	void k_means(shipments_t shipments);
};

#endif // OLAP_H
