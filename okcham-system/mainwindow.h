#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>

namespace Ui {
class MainWindow;
}

class MainWindow : public QMainWindow
{
	Q_OBJECT

	static const QStringList DIMENSIONS;

public:
	explicit MainWindow(QWidget *parent = 0);
	~MainWindow();

private slots:
	void on_button_set_etl_cron_clicked();
	void on_button_cron_notation_clicked();
	void on_button_run_ETL_clicked();

	void on_combo_x_currentIndexChanged(int index);
	void on_combo_y_currentIndexChanged(int index);
	void on_button_olap_clicked();

	void on_button_classify_clicked();
	void on_button_clasterize_clicked();

private:
	Ui::MainWindow *ui;

	void set_olap_dimensions();

	void perform_etl();
	void update_olap_combos(uint8_t combo_semantic, int index);

	std::string get_cron_parameters() const;
	std::string get_cron_statement() const;

	void update_etl_schedule(std::string options) const;
};

#endif // MAINWINDOW_H
