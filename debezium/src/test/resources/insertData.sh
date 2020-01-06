mongo -u debezium -p dbz --authenticationDatabase admin localhost:27017/inventory <<-EOF
    use inventory;
    db.customers.insert([
        { _id : NumberLong("1005"), first_name : 'Jason', last_name : 'Bourne', email : 'jason@bourne.org' }
    ]);
EOF
