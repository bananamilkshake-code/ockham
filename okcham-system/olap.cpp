#include "olap.h"

#include "mysql_connection_details.h"

#include <assert.h>
#include <sstream>

const QStringList OLAP::DETALIZATION[] =
{
	{"Year", "Month"},
	{"City", "Supplier"},
	{"HTP", "Detail"},
};

const std::vector<std::vector<std::string>> OLAP::ROW_NAMES =
{
	{"YEAR(sp.order_date)", "DATE_FORMAT(sp.order_date, %M, %Y)"},	// TIME
	{"s.city", "s.name"},						// PLACE
	{"p.htp", "p.name"}						// DETAIL
};

OLAP::OLAP()
{
	this->connection = mysql_init(nullptr);

	bool connection_succeed = mysql_real_connect(this->connection, DB_HOST, DB_USER, DB_PASSWORD, DB_TABLE, 0, nullptr, 0);
	assert(connection_succeed);
}

OLAP::~OLAP()
{
	mysql_close(this->connection);
}

OLAP::cube_t OLAP::calculate(OLAP::Type dim_1, uint8_t detalisation_1, OLAP::Type dim_2, uint8_t detalisation_2, OLAP::Type dim_3, uint8_t detalisation_3)
{
	std::string row_name_1 = "";
	std::string row_name_2 = "";
	std::string row_name_3 = "";

	std::stringstream cube_query;
	cube_query << "SELECT " <<
		row_name_1 << " AS dim_1 " <<
		row_name_2 << " AS dim_2 " <<
		row_name_3 << " AS dim_3 " <<
		 "FROM shipments sp "
		 "INNER JOIN suppliers s ON s.id = sp.sid "
		 "INNER JOIN parts s ON p.id = sp.pid "
		 "GROUP BY dim_1, dim_2, dim_3 WITH ROLLUP";

	if (mysql_query(this->connection, cube_query.str().c_str()))
		return cube_t();

	return this->convert_result(mysql_use_result(this->connection));
}

OLAP::values_list_t OLAP::get_suppliers() const
{
	values_list_t values;
	return values;
}

OLAP::values_list_t OLAP::get_cities() const
{
	values_list_t values;
	return values;
}

OLAP::values_list_t OLAP::get_details() const
{
	values_list_t values;
	return values;
}

OLAP::values_list_t OLAP::get_htp() const
{
	values_list_t values;
	return values;
}

OLAP::values_list_t OLAP::get_years() const
{
	values_list_t values;
	return values;
}

OLAP::values_list_t OLAP::get_monthes() const
{
	values_list_t values;
	return values;
}

OLAP::cube_t OLAP::convert_result(MYSQL_RES *result)
{
	return cube_t();
}
