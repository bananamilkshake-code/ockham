#include "etl.h"

static constexpr int8_t ERROR = -1;

bool ETL::run()
{
	auto res = system(std::string("ruby " + SCRIPT_PATH).c_str());
	return res != ERROR;
}

void ETL::set_cron(std::string params)
{

}