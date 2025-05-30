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

  it "does something" do
    user = User.create!(name: "Alice")
    10.times do |i|
      user.posts.create!(title: "Post #{i + 1}")
    end

    klass = aggregate do
      count(:total_posts) { |user| user.posts }
      count(:posts_with_title) { |user| user.posts.where(title: "Post 1") }
    end

    agg = klass.only(user)
    expect(agg.total_posts).to eq(10)
    expect(agg.posts_with_title).to eq(1)
  end
end
