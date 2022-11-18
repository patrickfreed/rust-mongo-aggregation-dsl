use mongodb::{
    bson::{bson, doc, Bson, Document},
    Client,
};

type Result<T> = anyhow::Result<T>;

trait Expression {
    fn into_bson(self) -> Bson;

    fn into_expr(self) -> Expr;
}

impl Expression for i32 {
    fn into_bson(self) -> Bson {
        self.into()
    }

    fn into_expr(self) -> Expr {
        Expr::Number(NumberExpr::Literal(self))
    }
}

impl NumberExpression for i32 {
    fn into_number_expr(self) -> NumberExpr {
        NumberExpr::Literal(self)
    }
}

trait BooleanExpression: Expression {}

trait NumberExpression: Expression {
    fn into_number_expr(self) -> NumberExpr;

    fn modulo(self, m: impl NumberExpression) -> NumberExpr
    where
        Self: Sized,
    {
        NumberExpr::Mod(
            Box::new(self.into_number_expr()),
            Box::new(m.into_number_expr()),
        )
    }

    fn eq(self, other: impl NumberExpression) -> BooleanExpr
    where
        Self: Sized,
    {
        BooleanExpr::Eq(Box::new(self.into_expr()), Box::new(other.into_expr()))
    }

    fn add(self, other: impl NumberExpression) -> NumberExpr
    where
        Self: Sized,
    {
        NumberExpr::Add(
            Box::new(self.into_number_expr()),
            Box::new(other.into_number_expr()),
        )
    }
}

trait IntegerExpression: Expression {}

#[derive(Debug, Clone)]
enum NumberExpr {
    Literal(i32),
    NumberField(NumberField),
    Mod(Box<NumberExpr>, Box<NumberExpr>),
    Reduce(NumberArrayExpr, Box<NumberExpr>, Box<NumberExpr>),
    Add(Box<NumberExpr>, Box<NumberExpr>),
}

impl Expression for NumberExpr {
    fn into_bson(self) -> Bson {
        match self {
            NumberExpr::Literal(i) => i.into(),
            NumberExpr::NumberField(f) => format!("${}", f.name()).into(),
            NumberExpr::Mod(lhs, rhs) => bson!({
                "$mod": [lhs.into_bson(), rhs.into_bson()]
            }),
            NumberExpr::Reduce(arr, iv, f) => bson!({
                "$reduce": {
                    "input": arr.into_bson(),
                    "initialValue": iv.into_bson(),
                    "in": f.into_bson(),
                }
            }),
            NumberExpr::Add(lhs, rhs) => bson!({
                "$add": [lhs.into_bson(), rhs.into_bson()]
            }),
        }
    }

    fn into_expr(self) -> Expr {
        Expr::Number(self)
    }
}

impl NumberExpression for NumberExpr {
    fn into_number_expr(self) -> NumberExpr {
        self
    }
}

#[derive(Debug, Clone)]
enum Expr {
    Boolean(BooleanExpr),
    Number(NumberExpr),
    NumberArray(NumberArrayExpr),
}

impl Expr {
    fn field_name(&self) -> Option<&str> {
        match self {
            Expr::Number(NumberExpr::NumberField(f)) => Some(f.name()),
            Expr::NumberArray(NumberArrayExpr::Field(f)) => Some(f.name()),
            _ => None,
        }
    }
}

impl Expression for Expr {
    fn into_bson(self) -> Bson {
        match self {
            Self::Boolean(b) => b.into_bson(),
            Self::NumberArray(f) => f.into_bson(),
            Self::Number(n) => n.into_bson(),
        }
    }

    fn into_expr(self) -> Expr {
        self
    }
}

type Number = i32;

#[derive(Debug, Clone)]
enum NumberArrayExpr {
    Field(NumberArrayField),
    Literal(Vec<Number>),
    Filter(Box<NumberArrayExpr>, BooleanExpr),
}

impl NumberArrayExpr {}

trait NumberArrayExpression {
    fn into_number_array_expr(self) -> NumberArrayExpr;

    fn reduce<F>(self, initial_value: impl NumberExpression, f: F) -> NumberExpr
    where
        F: FnOnce(NumberField, NumberField) -> NumberExpr,
        Self: Sized,
    {
        NumberExpr::Reduce(
            self.into_number_array_expr(),
            Box::new(initial_value.into_number_expr()),
            Box::new(f(NumberField::new("$this"), NumberField::new("$value"))),
        )
    }
}

impl Expression for NumberArrayExpr {
    fn into_bson(self) -> Bson {
        match self {
            NumberArrayExpr::Field(f) => format!("${}", f.name()).into(),
            NumberArrayExpr::Literal(a) => Bson::Array(a.into_iter().map(Bson::from).collect()),
            NumberArrayExpr::Filter(a, f) => {
                let expr = a.into_expr();

                bson!({
                    "$filter": {
                        "input": expr.into_bson(),
                        "cond": f.into_bson(),
                    }
                })
            }
        }
    }

    fn into_expr(self) -> Expr {
        Expr::NumberArray(self)
    }
}

impl NumberArrayExpression for NumberArrayExpr {
    fn into_number_array_expr(self) -> NumberArrayExpr {
        self
    }
}

impl NumberArrayExpression for NumberArrayField {
    fn into_number_array_expr(self) -> NumberArrayExpr {
        NumberArrayExpr::Field(self)
    }
}

#[derive(Debug, Clone)]
enum BooleanExpr {
    Eq(Box<Expr>, Box<Expr>),
}

impl Expression for BooleanExpr {
    fn into_bson(self) -> Bson {
        match self {
            BooleanExpr::Eq(lhs, rhs) => {
                bson!({
                    "$eq": [
                        lhs.into_bson(),
                        rhs.into_bson()
                    ]
                })
            }
        }
    }

    fn into_expr(self) -> Expr {
        Expr::Boolean(self)
    }
}

impl BooleanExpression for BooleanExpr {}

#[derive(Debug, Clone)]
struct NumberArrayField(String);

impl Expression for NumberArrayField {
    fn into_bson(self) -> Bson {
        Bson::String(self.0)
    }

    fn into_expr(self) -> Expr {
        Expr::NumberArray(NumberArrayExpr::Field(self))
    }
}
impl Field for NumberArrayField {
    type Type = NumberArrayExpr;

    fn name(&self) -> &str {
        self.0.as_str()
    }
}

impl NumberArrayField {
    fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    fn filter<F>(self, f: F) -> NumberArrayExpr
    where
        F: FnOnce(NumberField) -> BooleanExpr,
    {
        NumberArrayExpr::Filter(
            Box::new(NumberArrayExpr::Field(self)),
            f(NumberField::new("$this")),
        )
    }
}

#[derive(Debug, Clone)]
struct NumberField(String);

impl From<NumberField> for NumberExpr {
    fn from(i: NumberField) -> Self {
        NumberExpr::NumberField(i)
    }
}

impl NumberExpression for NumberField {
    fn into_number_expr(self) -> NumberExpr {
        self.into()
    }
}

impl NumberField {
    fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    fn eq(self, other: impl Into<NumberExpr>) -> BooleanExpr {
        BooleanExpr::Eq(
            Box::new(self.into_expr()),
            Box::new(other.into().into_expr()),
        )
    }
}

impl From<i32> for NumberExpr {
    fn from(i: i32) -> Self {
        NumberExpr::Literal(i)
    }
}

impl Expression for NumberField {
    fn into_bson(self) -> Bson {
        Bson::String(self.0)
    }

    fn into_expr(self) -> Expr {
        Expr::Number(self.into())
    }
}

trait Field {
    type Type: Expression;

    fn name(&self) -> &str;
}

impl Field for NumberField {
    type Type = NumberExpr;

    fn name(&self) -> &str {
        self.0.as_str()
    }
}

struct AggPipeline<S> {
    pipeline: Vec<Document>,
    schema: S,
}

impl<S: Clone> AggPipeline<S> {
    fn new(schema: S) -> Self {
        Self {
            pipeline: vec![],
            schema,
        }
    }

    fn agg_match<F>(mut self, expr: F) -> Self
    where
        F: FnOnce(S) -> BooleanExpr,
    {
        let computed_expr = expr(self.schema.clone());
        // let output = computed_expr.into_bson();
        let stage = match computed_expr {
            BooleanExpr::Eq(lhs, rhs) => {
                doc! {
                    "$match": {
                        lhs.field_name().unwrap(): rhs.into_bson()
                    }
                }
            }
        };
        self.pipeline.push(stage);
        self
    }

    fn add_field<FieldType, F, G>(mut self, field: FieldType, f: F) -> Self
    where
        FieldType: Field,
        G: Into<FieldType::Type>,
        F: FnOnce(S) -> G,
    {
        let computed_expr = f(self.schema.clone());
        let output = computed_expr.into().into_bson();
        self.pipeline.push(doc! {
            "$addFields": {
                field.name(): output
            }
        });
        self
    }

    fn unset<FieldType, F>(mut self, f: F) -> Self
    where
        FieldType: Field,
        F: FnOnce(S) -> FieldType,
    {
        let field = f(self.schema.clone());
        self.pipeline.push(doc! {
            "$project": {
                field.name(): 0
            }
        });
        self
    }
}

#[derive(Debug, Copy, Clone)]
struct Schema {}
impl Schema {
    fn id(self) -> NumberField {
        NumberField::new("_id")
    }

    fn num_list(self) -> NumberArrayField {
        NumberArrayField::new("numList")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let pipeline = AggPipeline::new(Schema {})
        .agg_match(|s| s.id().eq(5))
        .add_field(NumberField::new("output"), |s| {
            s.num_list()
                .filter(|n| n.modulo(2).eq(0))
                .reduce(0, |this, value| this.add(value))
        })
        .unset(|s| s.num_list());

    for stage in pipeline.pipeline.iter() {
        println!("{}", stage);
    }

    let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    let mut cursor = client
        .database("blah")
        .collection::<Document>("foo")
        .aggregate(pipeline.pipeline, None)
        .await?;

    while cursor.advance().await? {
        println!("{}", cursor.deserialize_current()?);
    }

    Ok(())
}
