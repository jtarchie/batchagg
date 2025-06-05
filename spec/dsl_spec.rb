# frozen_string_literal: true

require 'active_record'
ActiveRecord::Base.establish_connection(adapter: 'sqlite3', database: ':memory:')

RSpec.describe 'DSL' do
  with_model :User do
    table do |t|
      t.integer :age
      t.string :name
    end

    model do
      has_many :posts
    end
  end

  with_model :Post do
    table do |t|
      t.string :title
      t.integer :views, default: 0
      t.belongs_to :user
    end
    model do
      belongs_to :user
    end
  end

  it 'for a single user' do
    user = User.create!(name: 'Alice')
    10.times do |i|
      user.posts.create!(title: "Post #{i + 1}")
    end

    klass = aggregate(User) do # Pass User model
      count(:total_posts, &:posts)
      count(:posts_with_title) { |user_scope| user_scope.posts.where(title: 'Post 1') } # Renamed block arg
    end

    agg = klass.only(user)
    expect(agg[user.id].total_posts).to eq(10)
    expect(agg[user.id].posts_with_title).to eq(1)
  end

  it 'handles multiple users' do
    user1 = User.create!(name: 'Alice')
    user2 = User.create!(name: 'Bob')

    5.times { |i| user1.posts.create!(title: "Post #{i + 1}") }
    3.times { |i| user2.posts.create!(title: "Post #{i + 1}") }

    klass = aggregate(User) do # Pass User model
      count(:total_posts, &:posts)
      count(:posts_with_title) { |user_scope| user_scope.posts.where(title: 'Post 1') } # Renamed block arg
    end

    results = klass.from(User.all)
    expect(results[user1.id].total_posts).to eq(5)
    expect(results[user1.id].posts_with_title).to eq(1)

    expect(results[user2.id].total_posts).to eq(3)
    expect(results[user2.id].posts_with_title).to eq(1)
  end

  it 'handles different aggregate functions' do
    user = User.create!(name: 'Alice')

    # Add some posts with different values for aggregation
    user.posts.create!(title: 'Post 1', views: 100)
    user.posts.create!(title: 'Post 2', views: 200)
    user.posts.create!(title: 'Post 3', views: 300)

    klass = aggregate(User) do # Pass User model
      count(:total_posts, &:posts)
      sum(:total_views, :views, &:posts)
      avg(:avg_views, :views, &:posts)
      min(:min_views, :views, &:posts)
      max(:max_views, :views, &:posts)
    end

    agg = klass.only(user)
    expect(agg[user.id].total_posts).to eq(3)
    expect(agg[user.id].total_views).to eq(600)
    # Averages can be BigDecimal; convert to float for comparison if needed, or compare BigDecimal directly.
    # SQLite AVG returns float by default.
    expect(agg[user.id].avg_views.to_f).to eq(200.0)
    expect(agg[user.id].min_views).to eq(100)
    expect(agg[user.id].max_views).to eq(300)
  end

  it 'handles overlapping column names between models' do
    # Add a views column to User model to test conflict resolution
    User.connection.add_column User.table_name, :views, :integer, default: 0
    User.reset_column_information

    user = User.create!(name: 'Alice', views: 50)
    user.posts.create!(title: 'Post 1', views: 100)
    user.posts.create!(title: 'Post 2', views: 200)

    klass = aggregate(User) do # Pass User model
      sum(:total_post_views, :views, &:posts) # Should use posts.views, not users.views
    end

    agg = klass.only(user)
    expect(agg[user.id].total_post_views).to eq(300) # Sum of post views, not user views
  end

  it 'handles complex aggregations with expressions' do
    # Add a likes column to Post model for testing complex expressions
    Post.connection.add_column Post.table_name, :likes, :integer, default: 0
    Post.reset_column_information

    user = User.create!(name: 'Alice')
    user.posts.create!(title: 'Post 1', views: 100, likes: 10)
    user.posts.create!(title: 'Post 2', views: 200, likes: 20)
    user.posts.create!(title: 'Post 3', views: 150, likes: 15)

    klass = aggregate(User) do # Pass User model
      sum_expression(:total_engagement, 'views + likes', &:posts)
      sum_expression(:weighted_score, 'views * 2 + likes * 5', &:posts)
    end

    agg = klass.only(user)
    expect(agg[user.id].total_engagement).to eq(495) # (100+10) + (200+20) + (150+15)
    expect(agg[user.id].weighted_score).to eq(1125) # (100*2+10*5) + (200*2+20*5) + (150*2+15*5)
  end

  it 'handles distinct count aggregation' do
    user = User.create!(name: 'Alice')
    user.posts.create!(title: 'Post A', views: 100) # Assume Post has a category_id
    user.posts.create!(title: 'Post B', views: 200)
    user.posts.create!(title: 'Post C', views: 100)

    # Setup: Add category_id to posts for this test
    Post.connection.add_column Post.table_name, :category_id, :integer
    Post.reset_column_information
    user.posts.first.update!(category_id: 1)
    user.posts.second.update!(category_id: 2)
    user.posts.third.update!(category_id: 1)

    klass = aggregate(User) do
      count_distinct(:unique_categories, :category_id, &:posts)
      count_distinct(:distinct_view_counts, :views, &:posts)
    end

    agg = klass.only(user)
    expect(agg[user.id].unique_categories).to eq(2) # Categories 1 and 2
    expect(agg[user.id].distinct_view_counts).to eq(2) # View counts 100 and 200

    # Teardown: Remove category_id if it interferes with other tests
    # Or ensure it's scoped within this test or a context block
    Post.connection.remove_column Post.table_name, :category_id
    Post.reset_column_information
  end

  it 'handles string aggregation' do
    user = User.create!(name: 'Alice')
    user.posts.create!(title: 'First Post')
    user.posts.create!(title: 'Second Post')
    user.posts.create!(title: 'Third Post')

    klass = aggregate(User) do
      string_agg(:all_post_titles, :title, delimiter: '; ', &:posts)
    end

    agg = klass.only(user)
    # Order might not be guaranteed by default with GROUP_CONCAT unless an ORDER BY is in the subquery
    # For testing, we might need to sort the expected string or ensure the test data inserts in a fixed order
    # and the DB respects that for GROUP_CONCAT without explicit ordering.
    # For simplicity here, let's assume a consistent order or match as a set.
    titles = agg[user.id].all_post_titles.split('; ').sort
    expect(titles).to match_array(['First Post', 'Second Post', 'Third Post'])
  end

  it 'handles expression-based avg, min, max aggregations' do
    # Add columns for testing expressions
    Post.connection.add_column Post.table_name, :score, :integer, default: 0
    Post.connection.add_column Post.table_name, :bonus, :integer, default: 0
    Post.reset_column_information

    user = User.create!(name: 'Alice')
    user.posts.create!(title: 'Post 1', score: 10, bonus: 1) # score + bonus = 11
    user.posts.create!(title: 'Post 2', score: 20, bonus: 2) # score + bonus = 22
    user.posts.create!(title: 'Post 3', score: 30, bonus: 3) # score + bonus = 33

    klass = aggregate(User) do
      avg_expression(:avg_total_score, 'score + bonus', &:posts)
      min_expression(:min_total_score, 'score + bonus', &:posts)
      max_expression(:max_total_score, 'score + bonus', &:posts)
      avg_expression(:avg_score_times_two, 'score * 2', &:posts)
      min_expression(:min_score_plus_five, 'score + 5', &:posts)
      max_expression(:max_bonus_doubled, 'bonus * 2', &:posts)
    end

    agg = klass.only(user)

    # (11 + 22 + 33) / 3 = 66 / 3 = 22
    expect(agg[user.id].avg_total_score.to_f).to eq(22.0)
    expect(agg[user.id].min_total_score).to eq(11) # min(11, 22, 33)
    expect(agg[user.id].max_total_score).to eq(33) # max(11, 22, 33)

    # (10*2 + 20*2 + 30*2) / 3 = (20 + 40 + 60) / 3 = 120 / 3 = 40
    expect(agg[user.id].avg_score_times_two.to_f).to eq(40.0)
    # min(10+5, 20+5, 30+5) = min(15, 25, 35)
    expect(agg[user.id].min_score_plus_five).to eq(15)
    # max(1*2, 2*2, 3*2) = max(2, 4, 6)
    expect(agg[user.id].max_bonus_doubled).to eq(6)

    # Teardown
    Post.connection.remove_column Post.table_name, :score
    Post.connection.remove_column Post.table_name, :bonus
    Post.reset_column_information
  end

  it 'handles with column on model' do
    Post.connection.add_column Post.table_name, :likes, :integer, default: 0
    Post.reset_column_information

    user = User.create!(name: 'Alice', age: 30)
    user.posts.create!(title: 'Post 1', views: 100, likes: 10)
    user.posts.create!(title: 'Post 2', views: 200, likes: 20)
    user.posts.create!(title: 'Post 3', views: 150, likes: 15)

    klass = aggregate(User) do # Pass User model
      column(:age)
      sum_expression(:total_engagement, 'views + likes', &:posts)
      sum_expression(:weighted_score, 'views * 2 + likes * 5', &:posts)
    end

    agg = klass.only(user)
    expect(agg[user.id].total_engagement).to eq(495) # (100+10) + (200+20) + (150+15)
    expect(agg[user.id].weighted_score).to eq(1125) # (100*2+10*5) + (200*2+20*5) + (150*2+15*5)
    expect(agg[user.id].age).to eq(30)
  end
end
