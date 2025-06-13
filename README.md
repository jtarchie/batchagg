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

## Installation

```ruby
gem 'batchagg'
```

## Usage

### Example 1: Aggregations for a single user

```ruby
# Define the aggregations you need
user_stats = aggregate(User) do
  count(:total_posts, &:posts)
  count(:published_posts) { |user| user.posts.where(status: 'published') }
  sum(:total_views, :views, &:posts)
  avg(:avg_rating, :rating, &:posts)
end

# Get aggregations for a specific user
user = User.find(1)
stats = user_stats.only(user)

puts "Total posts: #{stats[user.id].total_posts}"
puts "Published posts: #{stats[user.id].published_posts}"
puts "Total views: #{stats[user.id].total_views}"
puts "Average rating: #{stats[user.id].avg_rating}"
```

### Example 2: Aggregations for multiple users

```ruby
# Define the aggregations you need
user_stats = aggregate(User) do
  count(:total_posts, &:posts)
  count(:comments_received) { |user| user.posts.joins(:comments) }
  string_agg(:post_titles, :title, delimiter: ', ', &:posts)
  max(:highest_rating, :rating, &:posts)
end

# Get aggregations for all active users
active_users = User.where(active: true)
stats = user_stats.from(active_users)

# Use the aggregated data
active_users.each do |user|
  user_stat = stats[user.id]
  puts "User #{user.name} has #{user_stat.total_posts} posts"
  puts "Most recent post titles: #{user_stat.post_titles}"
  puts "Highest rating: #{user_stat.highest_rating}"
end
```

## Supported Aggregation Types

- `count`: Count of records
- `count_distinct`: Count of distinct values for a column
- `sum`: Sum of a column
- `avg`: Average of a column
- `min`: Minimum value of a column
- `max`: Maximum value of a column
- `string_agg`: Concatenation of values (GROUP_CONCAT)

You can also use the `_expression` variants of these methods for custom SQL
expressions.
