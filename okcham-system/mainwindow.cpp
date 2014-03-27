#include "mainwindow.h"
#include "ui_mainwindow.h"

#include <QMessageBox>

MainWindow::MainWindow(QWidget *parent) :
    QMainWindow(parent),
    ui(new Ui::MainWindow)
{
    ui->setupUi(this);
}

MainWindow::~MainWindow()
{
    delete ui;
}

void MainWindow::on_button_set_etl_cron_clicked()
{
    QMessageBox Msgbox;
    Msgbox.setText("sum of numbers are...."+0);
    Msgbox.exec();
}
