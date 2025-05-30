require "active_record"
ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")

RSpec.describe "DSL" do
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
      t.belongs_to :user
    end
    model do
      belongs_to :user
    end
  end

  it "for a single user" do
    user = User.create!(name: "Alice")
    10.times do |i|
      user.posts.create!(title: "Post #{i + 1}")
    end

    klass = aggregate do
      count(:total_posts) { |user| user.posts }
      count(:posts_with_title) { |user| user.posts.where(title: "Post 1") }
    end

    agg = klass.only(user)
    expect(agg[user.id].total_posts).to eq(10)
    expect(agg[user.id].posts_with_title).to eq(1)
  end

  it "handles multiple users" do
    user1 = User.create!(name: "Alice")
    user2 = User.create!(name: "Bob")

    5.times { |i| user1.posts.create!(title: "Post #{i + 1}") }
    3.times { |i| user2.posts.create!(title: "Post #{i + 1}") }

    klass = aggregate do
      count(:total_posts) { |user| user.posts }
      count(:posts_with_title) { |user| user.posts.where(title: "Post 1") }
    end

    results = klass.from(User.all)
    expect(results[user1.id].total_posts).to eq(5)
    expect(results[user1.id].posts_with_title).to eq(1)

    expect(results[user2.id].total_posts).to eq(3)
    expect(results[user2.id].posts_with_title).to eq(1)
  end
end
