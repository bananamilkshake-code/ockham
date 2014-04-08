#ifndef OLAP_H
#define OLAP_H

#include <list>
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
		float weight = 0.0;
		float detail_weight = 0.0;
		float price = 0.0;
		float detail_price = 0.0;
		uint64_t quantity = 0;
		std::string detail_name = "";
		std::string city = "";
		bool htp = false;

		const Shipment& operator+=(const Shipment &other);
		const Shipment operator/(const float denominator);
		bool operator<(const Shipment &other) const;
		bool operator==(const Shipment &other) const;

		float hash() const;
	};
	typedef std::set<Shipment> shipments_t;

	struct Cluster
	{
		shipments_t elements;

		Cluster(Shipment center): m(center) {}

		void recalc_position();

		inline bool operator==(const Cluster &other) const { return this->elements == other.elements; }
		inline const Shipment& center() const { return this->m; }

	private:
		Shipment m;
	};
	typedef std::list<Cluster> clusters_t;

	typedef std::map<std::string, std::map<std::string, std::string>> cube_t;

	static const QStringList DETALIZATION[];
	static const std::vector<std::vector<std::string>> ROW_NAMES;

	static const std::string ALL;

	static float distance(const Shipment &s1, const Shipment &s2);

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

	void k_means(const shipments_t &shipments) const;
};

#endif // OLAP_H
