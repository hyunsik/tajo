pub fn main() {
  let coord_config = Config::from_str(r#"
[coordinator]
enabled = true
uri="http://localhost:8080"

[node]

[node.http]
listen="127.0.0.1:8080"

[node.executor]
data_dir="/tmp/flang/data"
"#).unwrap();

  let mut builder = LocalClusterBuilder::new();
  builder.add_node(coord_config);
  let mut cluster = builder.build();

  cluster.startup().ok();
}