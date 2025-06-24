# BatchAgg

BatchAgg is a Ruby gem for efficiently performing multiple database aggregations
on ActiveRecord models in a single query. It helps eliminate N+1 query problems
when calculating counts, sums, averages, and other aggregates across
associations.

## Purpose

BatchAgg addresses the common issue of needing multiple aggregation values for a
collection of records without making repeated database queries. It uses
correlated subqueries to fetch all aggregations in a single efficient database
call, improving application performance.

### Unsupported

Because of the composing of SQL statements, there will never be support for
`group` or `GROUP BY`. Please don't open an issue about this, unless you have an
idea to solve it.

## Installation

```ruby
gem 'batchagg'
```

## Usage

BatchAgg lets you define multiple aggregations on ActiveRecord models and fetch
them efficiently in a single query. You can use it for counts, sums, averages,
min/max, string aggregation, and even computed fields.

### Basic Example: Aggregations for a Single User

```ruby
include BatchAgg::DSL

user_stats = aggregate(User) do
  count(:total_posts, &:posts)
  count(:published_posts) { |user| user.posts.where(status: 'published') }
  sum(:total_views, :views, &:posts)
  avg(:avg_rating, :rating, &:posts)
end

user = User.find(1)
stats = user_stats.only(user)

puts stats[user.id].total_posts
puts stats[user.id].published_posts
puts stats[user.id].total_views
puts stats[user.id].avg_rating
```

### Aggregations for Multiple Records

```ruby
include BatchAgg::DSL

user_stats = aggregate(User) do
  count(:total_posts, &:posts)
  count(:comments_received) { |user| user.posts.joins(:comments) }
  string_agg(:post_titles, :title, delimiter: ', ', &:posts)
  max(:highest_rating, :rating, &:posts)
end

active_users = User.where(active: true)
stats = user_stats.from(active_users)

active_users.each do |user|
  puts "#{user.name}: #{stats[user.id].total_posts} posts, titles: #{stats[user.id].post_titles}"
end
```

### Supported Aggregation Types

- `count`, `count_distinct`
- `sum`
- `avg`
- `min`, `max`
- `string_agg` (concatenates values, e.g. post titles)
- All above also support `_expression` variants for custom SQL expressions.

### Advanced: Computed Fields

You can define computed fields that use the results of other aggregations:

```ruby
user_stats = aggregate(User) do
  count(:total_posts, &:posts)
  sum(:total_views, :views, &:posts)
  computed(:score) do |result|
    result.total_posts * 10 + result.total_views
  end
end

stats = user_stats.only(user)
puts stats[user.id].score
```

### Associations

BatchAgg supports aggregations over:

- `has_many`, `has_one`, `belongs_to`
- `has_many :through`
- Custom scopes and joins

Example for `has_many :through`:

```ruby
physician_stats = aggregate(Physician) do
  count(:total_patients, &:patients)
  string_agg(:patient_names, :name, delimiter: ', ', &:patients)
end

stats = physician_stats.from(Physician.all)
```

### Passing Parameters

You can pass parameters to your aggregation blocks:

```ruby
user_stats = aggregate(User) do
  count(:posts_with_title) { |user, title:| user.posts.where(title: title) }
end

stats = user_stats.only(user, title: "Hello World")
```

### Handling NULLs

Aggregations like `sum`, `avg`, `min`, `max` default to `0` (or `0.0` for
averages) if there are no matching records.

NOTE: This could change in the future, as I'm not sure how it affects others
code bases.

## Database Compatibility

BatchAgg supports:

- **PostgreSQL**
- **MySQL**
- **SQLite**

String aggregation and some SQL expressions are adapted for each database. All
tests run against all three databases.

## Known Limitations

- **No GROUP BY support:** BatchAgg does not support SQL `GROUP BY` or `.group`
  in ActiveRecord. It is designed for correlated subqueries per record.
- **No support for eager loading:** BatchAgg is not a replacement for
  `.includes` or `.preload`.
- **Custom SQL expressions:** Use with care and ensure expressions are
  compatible with your database.
- **Aggregations only:** BatchAgg is not for fetching associated records, only
  for aggregate values.

If you have ideas for overcoming these limitations, contributions are welcome!
