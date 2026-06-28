# GoData

A lightweight, lazy DataFrame library for Go, built on DuckDB.  
Inspired by pandas, with a focus on simplicity and performance.

## Features

- **Lazy evaluation** — Operations build SQL queries that only run when you read data
- **Multiple data sources** — CSV, JSON, Parquet, maps, and structs
- **Rich transformations** — Select, Filter, GroupBy, Join, Sort, WithColumn, etc.
- **Fast aggregations** using DuckDB's engine
- **Easy to use** — Familiar pandas-like API

## Installation

```bash
go get github.com/DomArruda/GoData
