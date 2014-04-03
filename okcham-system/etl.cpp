#include "etl.h"

const std::string ETL::SCRIPT_PATH = "/home/vnuchka/Documents/Projects/okcham/etl.rb";

static constexpr int8_t ERROR = -1;

bool ETL::run()
{
	auto res = system(std::string("ruby " + SCRIPT_PATH).c_str());
	return (res != ERROR);
}

void ETL::set_cron(std::string options)
{

}
