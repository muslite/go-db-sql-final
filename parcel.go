package main

import (
	"database/sql"
	// "golang.org/x/crypto/openpgp/errors"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	res, err := s.db.Exec("INSERT INTO parcel (client,status,address,created_at) VALUES (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt),
	)
	if err != nil {
		return 0, err
	}

	// получаем идентификатор добавленной записи если это возможно.
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	// верните идентификатор последней добавленной записи
	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	row := s.db.QueryRow("SELECT number,client,status,address,created_at FROM parcel WHERE number=:number",
		sql.Named("number", number))
	// заполните объект Parcel данными из таблицы
	p := Parcel{}
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		ep := Parcel{}
		return ep, err
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	var res []Parcel
	var eres []Parcel // пустой слайс структуры ю возвращаем его при ошибках.

	rows, err := s.db.Query("SELECT number,client,status,address,created_at FROM parcel WHERE client=:client",
		sql.Named("client", client))
	if err != nil {
		return eres, err // в данном случак одинакоко что вернуть res или eres, поскольку оба пока пусты.
	}
	defer rows.Close()
	// заполните срез Parcel данными из таблицы
	// var res []Parcel

	for rows.Next() {
		var row_res Parcel

		err := rows.Scan(&row_res.Number, &row_res.Client, &row_res.Status, &row_res.Address, &row_res.CreatedAt)

		if err != nil {
			return eres, err
		}
		res = append(res, row_res)
	}
	// http://go-database-sql.org/errors.html  В модуле есть, правда без акцента и/или объяснения для чего.
	if err := rows.Err(); err != nil {
		return eres, err
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel
	_, err := s.db.Exec("UPDATE parcel SET status=:status WHERE number=:number",
		sql.Named("status", status),
		sql.Named("number", number),
	)
	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	/*
		row := s.db.QueryRow("SELECT status FROM parcel WHERE number=:number", sql.Named("number", number))

		var status string
		err := row.Scan(&status)
		if err != nil {
			return err
		}

		if status != ParcelStatusRegistered {
			err := errors.New("Unexpected status of shipment")
			return err
		}
	*/
	_, err := s.db.Exec("UPDATE parcel SET address=:address WHERE number=:number AND status=:status",
		sql.Named("address", address),
		sql.Named("number", number),
		sql.Named("status", ParcelStatusRegistered),
	)
	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	/*
		row := s.db.QueryRow("SELECT status FROM parcel WHERE number=:number", sql.Named("number", number))

		var status string
		err := row.Scan(&status)
		if err != nil {
			return err
		}
		//fmt.Println(status)
		if status != ParcelStatusRegistered {
			err := errors.New("Unexpected status of shipment")
			return err
		}
		//fmt.Println("Run Delete")
	*/
	_, err := s.db.Exec("DELETE FROM parcel WHERE number=:number AND status=:status",
		sql.Named("number", number),
		sql.Named("status", ParcelStatusRegistered),
	)
	if err != nil {
		return err
	}

	return nil
}
