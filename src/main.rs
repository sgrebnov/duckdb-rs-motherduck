use duckdb::Connection;
use duckdb::arrow::util::pretty::print_batches;

fn main() {

    println!("Attempt to connect using the motherduck extension.");

    let motherduck_token =  "to_be_replaced_with_actual_token";
    
    let conn_params = format!("md:?motherduck_token={motherduck_token}&saas_mode=true");

    match Connection::open(conn_params) {
        Ok(conn) => {
            println!("Connected!");

            let mut stmt = conn
                .prepare("SELECT * FROM sample_data.nyc.taxi LIMIT 1")
                .unwrap();

            let result: duckdb::Arrow<'_> = stmt
                .query_arrow([]).unwrap();

            println!("Schema: {:?}", result.get_schema());
            println!();
            for batch in result {
                let _ = print_batches(&[batch]);
            }
        }
        Err(e) => {
            println!("Error: {:?}", e);
        }
    }
}
