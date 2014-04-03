#include "mainwindow.h"
#include "ui_mainwindow.h"

#include <array>
#include <regex>
#include <sstream>

#include <QMessageBox>

#include "etl.h"

const QStringList MainWindow::DIMENSIONS =
{
	"Time",
	"Place",
	"Detail"
};

MainWindow::MainWindow(QWidget *parent) :
	QMainWindow(parent),
	ui(new Ui::MainWindow)
{
	ui->setupUi(this);

	this->set_olap_dimensions();
}

MainWindow::~MainWindow()
{
	delete ui;
}

void MainWindow::on_button_set_etl_cron_clicked()
{
	this->update_etl_schedule(this->get_cron_parameters());
}

void MainWindow::on_button_cron_notation_clicked()
{
	this->update_etl_schedule(this->get_cron_statement());
}

void MainWindow::on_button_run_ETL_clicked()
{
	this->perform_etl();
}

void MainWindow::set_olap_dimensions()
{
	this->ui->combo_x->addItems(DIMENSIONS);
	this->ui->combo_y->addItems(DIMENSIONS);

	this->ui->combo_x->setCurrentIndex(0);
	this->update_olap_combos('x', 0);
}

void MainWindow::perform_etl()
{
	this->ui->text_etl_process->insertPlainText("Start ETL process\n");

	auto success = ETL::run();
	if (!success)
		this->ui->text_etl_process->insertPlainText("Error on ETL performance: check script path to \"etl.rb\"\n");
	else
		this->ui->text_etl_process->insertPlainText("End ETL process\n");
}

void MainWindow::update_olap_combos(uint8_t combo_semantic, int index)
{
	auto other_combo = (combo_semantic == 'x' ? this->ui->combo_y : this->ui->combo_x);

	if (other_combo->currentIndex() == index)
		other_combo->setCurrentIndex((index + 1) % DIMENSIONS.size());

	uint8_t z_dimension = 0;
	while (z_dimension == this->ui->combo_x->currentIndex() || z_dimension == this->ui->combo_y->currentIndex())
		z_dimension++;

	this->ui->label_z->setText(DIMENSIONS.at(z_dimension));
}

static constexpr int8_t EVERY = -1;
static constexpr char *EVERY_CHAR = "*";

std::string MainWindow::get_cron_parameters() const
{
	std::ostringstream stream_parameters;

	const int8_t time_periods[] =
	{
		this->ui->spin_minute->value(),
		this->ui->spin_hour->value(),
		this->ui->spin_month_day->value(),
		this->ui->spin_month->value(),
		this->ui->spin_week_day->value()
	};

	for (auto value : time_periods)
	{
		if (stream_parameters.tellp())
			stream_parameters << " ";
		(value == EVERY) ? stream_parameters << EVERY_CHAR : stream_parameters << (int32_t)value;
	}

	return stream_parameters.str();
}

std::string MainWindow::get_cron_statement() const
{
	return this->ui->line_cron->text().toUtf8().constData();
}

void MainWindow::update_etl_schedule(std::string options) const
{
	std::regex cron_statement_regex("");

	if (!std::regex_search(options, cron_statement_regex, std::regex_constants::match_continuous))
	{
		QMessageBox error_statement;
		error_statement.setText("Wrong error statement for cron \"" + QString(options.c_str()) + "\"");
		error_statement.exec();
		return;
	}

	ETL::set_cron(options);
}

void MainWindow::on_combo_x_currentIndexChanged(int index)
{
	this->update_olap_combos('x', index);
}

void MainWindow::on_combo_y_currentIndexChanged(int index)
{
	this->update_olap_combos('y', index);
}

void MainWindow::on_button_olap_clicked()
{
}

void MainWindow::on_button_classify_clicked()
{
}

void MainWindow::on_button_clasterize_clicked()
{
}
