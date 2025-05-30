# frozen_string_literal: true

require 'active_record'
ActiveRecord::Base.establish_connection(adapter: 'sqlite3', database: ':memory:')

RSpec.describe 'DSL' do
  with_model :User do
    table do |t|
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

    klass = aggregate do
      count(:total_posts, &:posts)
      count(:posts_with_title) { |user| user.posts.where(title: 'Post 1') }
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

    klass = aggregate do
      count(:total_posts, &:posts)
      count(:posts_with_title) { |user| user.posts.where(title: 'Post 1') }
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

    klass = aggregate do
      count(:total_posts, &:posts)
      sum(:total_views, :views, &:posts)
      avg(:avg_views, :views, &:posts)
      min(:min_views, :views, &:posts)
      max(:max_views, :views, &:posts)
    end

    agg = klass.only(user)
    expect(agg[user.id].total_posts).to eq(3)
    expect(agg[user.id].total_views).to eq(600)
    expect(agg[user.id].avg_views).to eq(200.0)
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

    klass = aggregate do
      sum(:total_post_views, :views, &:posts) # Should use posts.views, not users.views
    end

    agg = klass.only(user)
    expect(agg[user.id].total_post_views).to eq(300) # Sum of post views, not user views
  end
end
