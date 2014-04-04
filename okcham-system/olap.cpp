#include "olap.h"

#include "mysql_connection_details.h"

#include <assert.h>
#include <sstream>

std::pair<std::string, std::string> rows[] = {
	{"month", "year"},
	{"place", "city"},
	{"detail", "HTP"},
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

OLAP::cube_t OLAP::calculate(uint8_t dim_1, uint8_t dim_2, uint8_t dim_3)
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

OLAP::cube_t OLAP::convert_result(MYSQL_RES *result)
{
	return cube_t();
}
