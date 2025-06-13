# frozen_string_literal: true

require "active_record"
require "rspec-sqlimit"
ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")

RSpec.describe "DSL" do
  include BatchAgg::DSL

  with_model :User do
    table do |t|
      t.integer :age
      t.string :name
    end

    model do
      has_many :posts
      has_one :profile
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

  with_model :Category do
    table do |t|
      t.string :name
      t.integer :priority, default: 0
    end
    model do
      has_many :posts
    end
  end

  with_model :Profile do
    table do |t|
      t.string :bio
      t.boolean :verified, default: false
      t.integer :views, default: 0
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

    klass = aggregate(User) do # Pass User model
      count(:total_posts, &:posts)
      count(:posts_with_title) { |user_scope| user_scope.posts.where(title: "Post 1") } # Renamed block arg
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    expect(@agg[user.id].total_posts).to eq(10)
    expect(@agg[user.id].posts_with_title).to eq(1)
  end

  it "handles multiple users" do
    user1 = User.create!(name: "Alice")
    user2 = User.create!(name: "Bob")

    5.times { |i| user1.posts.create!(title: "Post #{i + 1}") }
    3.times { |i| user2.posts.create!(title: "Post #{i + 1}") }

    klass = aggregate(User) do # Pass User model
      count(:total_posts, &:posts)
      count(:posts_with_title) { |user_scope| user_scope.posts.where(title: "Post 1") } # Renamed block arg
    end

    expect do
      @results = klass.from(User.all)
    end.to_not exceed_query_limit(1)

    expect(@results[user1.id].total_posts).to eq(5)
    expect(@results[user1.id].posts_with_title).to eq(1)

    expect(@results[user2.id].total_posts).to eq(3)
    expect(@results[user2.id].posts_with_title).to eq(1)
  end

  it "handles different aggregate functions" do
    user = User.create!(name: "Alice")

    # Add some posts with different values for aggregation
    user.posts.create!(title: "Post 1", views: 100)
    user.posts.create!(title: "Post 2", views: 200)
    user.posts.create!(title: "Post 3", views: 300)

    klass = aggregate(User) do # Pass User model
      count(:total_posts, &:posts)
      sum(:total_views, :views, &:posts)
      avg(:avg_views, :views, &:posts)
      min(:min_views, :views, &:posts)
      max(:max_views, :views, &:posts)
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    expect(@agg[user.id].total_posts).to eq(3)
    expect(@agg[user.id].total_views).to eq(600)
    # Averages can be BigDecimal; convert to float for comparison if needed, or compare BigDecimal directly.
    # SQLite AVG returns float by default.
    expect(@agg[user.id].avg_views.to_f).to eq(200.0)
    expect(@agg[user.id].min_views).to eq(100)
    expect(@agg[user.id].max_views).to eq(300)
  end

  it "handles overlapping column names between models" do
    # Add a views column to User model to test conflict resolution
    User.connection.add_column User.table_name, :views, :integer, default: 0
    User.reset_column_information

    user = User.create!(name: "Alice", views: 50)
    user.posts.create!(title: "Post 1", views: 100)
    user.posts.create!(title: "Post 2", views: 200)

    klass = aggregate(User) do # Pass User model
      sum(:total_post_views, :views, &:posts) # Should use posts.views, not users.views
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    expect(@agg[user.id].total_post_views).to eq(300) # Sum of post views, not user views
  end

  it "handles complex aggregations with expressions" do
    # Add a likes column to Post model for testing complex expressions
    Post.connection.add_column Post.table_name, :likes, :integer, default: 0
    Post.reset_column_information

    user = User.create!(name: "Alice")
    user.posts.create!(title: "Post 1", views: 100, likes: 10)
    user.posts.create!(title: "Post 2", views: 200, likes: 20)
    user.posts.create!(title: "Post 3", views: 150, likes: 15)

    klass = aggregate(User) do # Pass User model
      sum_expression(:total_engagement, "views + likes", &:posts)
      sum_expression(:weighted_score, "views * 2 + likes * 5", &:posts)
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    expect(@agg[user.id].total_engagement).to eq(495) # (100+10) + (200+20) + (150+15)
    expect(@agg[user.id].weighted_score).to eq(1125) # (100*2+10*5) + (200*2+20*5) + (150*2+15*5)
  end

  it "handles distinct count aggregation" do
    user = User.create!(name: "Alice")
    user.posts.create!(title: "Post A", views: 100) # Assume Post has a category_id
    user.posts.create!(title: "Post B", views: 200)
    user.posts.create!(title: "Post C", views: 100)

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

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    expect(@agg[user.id].unique_categories).to eq(2) # Categories 1 and 2
    expect(@agg[user.id].distinct_view_counts).to eq(2) # View counts 100 and 200
  end

  it "handles string aggregation" do
    user = User.create!(name: "Alice")
    user.posts.create!(title: "First Post")
    user.posts.create!(title: "Second Post")
    user.posts.create!(title: "Third Post")

    klass = aggregate(User) do
      string_agg(:all_post_titles, :title, delimiter: "; ", &:posts)
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    titles = @agg[user.id].all_post_titles.split("; ").sort
    expect(titles).to match_array(["First Post", "Second Post", "Third Post"])
  end

  it "handles expression-based avg, min, max aggregations" do
    # Add columns for testing expressions
    Post.connection.add_column Post.table_name, :score, :integer, default: 0
    Post.connection.add_column Post.table_name, :bonus, :integer, default: 0
    Post.reset_column_information

    user = User.create!(name: "Alice")
    user.posts.create!(title: "Post 1", score: 10, bonus: 1) # score + bonus = 11
    user.posts.create!(title: "Post 2", score: 20, bonus: 2) # score + bonus = 22
    user.posts.create!(title: "Post 3", score: 30, bonus: 3) # score + bonus = 33

    klass = aggregate(User) do
      avg_expression(:avg_total_score, "score + bonus", &:posts)
      min_expression(:min_total_score, "score + bonus", &:posts)
      max_expression(:max_total_score, "score + bonus", &:posts)
      avg_expression(:avg_score_times_two, "score * 2", &:posts)
      min_expression(:min_score_plus_five, "score + 5", &:posts)
      max_expression(:max_bonus_doubled, "bonus * 2", &:posts)
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    # (11 + 22 + 33) / 3 = 66 / 3 = 22
    expect(@agg[user.id].avg_total_score.to_f).to eq(22.0)
    expect(@agg[user.id].min_total_score).to eq(11) # min(11, 22, 33)
    expect(@agg[user.id].max_total_score).to eq(33) # max(11, 22, 33)

    # (10*2 + 20*2 + 30*2) / 3 = (20 + 40 + 60) / 3 = 120 / 3 = 40
    expect(@agg[user.id].avg_score_times_two.to_f).to eq(40.0)
    # min(10+5, 20+5, 30+5) = min(15, 25, 35)
    expect(@agg[user.id].min_score_plus_five).to eq(15)
    # max(1*2, 2*2, 3*2) = max(2, 4, 6)
    expect(@agg[user.id].max_bonus_doubled).to eq(6)
  end

  it "handles with column on model" do
    Post.connection.add_column Post.table_name, :likes, :integer, default: 0
    Post.reset_column_information

    user = User.create!(name: "Alice", age: 30)
    user.posts.create!(title: "Post 1", views: 100, likes: 10)
    user.posts.create!(title: "Post 2", views: 200, likes: 20)
    user.posts.create!(title: "Post 3", views: 150, likes: 15)

    klass = aggregate(User) do # Pass User model
      column(:age)
      column(:aliased_aged, &:age)
      column(:views) { |user_scope| user_scope.posts.limit(1).select(:views) }
      sum_expression(:total_engagement, "views + likes", &:posts)
      sum_expression(:weighted_score, "views * 2 + likes * 5", &:posts)
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    expect(@agg[user.id].total_engagement).to eq(495) # (100+10) + (200+20) + (150+15)
    expect(@agg[user.id].weighted_score).to eq(1125) # (100*2+10*5) + (200*2+20*5) + (150*2+15*5)
    expect(@agg[user.id].age).to eq(30)
    expect(@agg[user.id].aliased_aged).to eq(30) # Add this line to verify the aliased column
    expect(@agg[user.id].views).to eq(100) # Add this line to verify the aliased column
  end

  it "handles complex joins and includes in aggregations" do
    # Add category_id to posts
    Post.connection.add_column Post.table_name, :category_id, :integer
    Post.connection.add_column Post.table_name, :published, :boolean, default: true
    Post.reset_column_information
    Post.class_eval { belongs_to :category, optional: true }

    # Create test data
    tech_category = Category.create!(name: "Technology", priority: 1)
    sports_category = Category.create!(name: "Sports", priority: 2)

    user = User.create!(name: "Alice", age: 30)

    # Create posts with categories
    user.posts.create!(title: "Tech Post 1", views: 100, category: tech_category, published: true)
    user.posts.create!(title: "Tech Post 2", views: 200, category: tech_category, published: false)
    user.posts.create!(title: "Sports Post 1", views: 150, category: sports_category, published: true)
    user.posts.create!(title: "Uncategorized Post", views: 50, published: true) # No category

    klass = aggregate(User) do
      column(:age)

      # Count posts by joining with categories
      count(:published_tech_posts) do |user_scope|
        user_scope.posts
                  .joins(:category)
                  .where(Category.table_name => { name: "Technology" }, published: true)
      end

      # Sum views for high-priority categories using includes
      sum(:high_priority_views, :views) do |user_scope|
        user_scope.posts
                  .joins(:category)
                  .where(Category.table_name => { priority: [1, 2] })
      end

      # Complex expression with joined data
      sum_expression(:category_weighted_score,
                     "#{Post.table_name}.views * #{Category.table_name}.priority") do |user_scope|
        user_scope.posts.joins(:category)
      end
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    expect(@agg[user.id].age).to eq(30)
    expect(@agg[user.id].published_tech_posts).to eq(1) # Only Tech Post 1 is published
    expect(@agg[user.id].high_priority_views).to eq(450) # 100 + 200 + 150 (all categorized posts)
    expect(@agg[user.id].category_weighted_score).to eq(600) # (100*1) + (200*1) + (150*2)
  end

  it "handles count_expression aggregations" do
    # Add columns for testing count expressions
    Post.connection.add_column Post.table_name, :status, :string, default: "draft"
    Post.connection.add_column Post.table_name, :score, :integer, default: 0
    Post.reset_column_information

    user = User.create!(name: "Alice")
    user.posts.create!(title: "Post 1", status: "published", score: 5)
    user.posts.create!(title: "Post 2", status: "draft", score: 3)
    user.posts.create!(title: "Post 3", status: "published", score: 7)
    user.posts.create!(title: "Post 4", status: "published", score: 2)

    klass = aggregate(User) do
      count_expression(:published_posts, "CASE WHEN status = 'published' THEN 1 END", &:posts)
      count_expression(:high_score_posts, "CASE WHEN score > 5 THEN 1 END", &:posts)
      count_expression(:complex_condition, "CASE WHEN status = 'published' AND score >= 5 THEN 1 END", &:posts)
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    expect(@agg[user.id].published_posts).to eq(3) # Posts 1, 3, 4 are published
    expect(@agg[user.id].high_score_posts).to eq(1) # Only Post 3 has score > 5
    expect(@agg[user.id].complex_condition).to eq(2) # Posts 1 and 3 meet both conditions
  end

  it "handles count_distinct_expression aggregations" do
    # Add columns for testing distinct count expressions
    Post.connection.add_column Post.table_name, :author_name, :string
    Post.connection.add_column Post.table_name, :topic, :string
    Post.reset_column_information

    user = User.create!(name: "Alice")
    user.posts.create!(title: "Post 1", author_name: "John", topic: "Tech")
    user.posts.create!(title: "Post 2", author_name: "Jane", topic: "Tech")
    user.posts.create!(title: "Post 3", author_name: "John", topic: "Sports")
    user.posts.create!(title: "Post 4", author_name: "Bob", topic: "Tech")

    klass = aggregate(User) do
      count_distinct_expression(:unique_author_topic_pairs, "author_name || '-' || topic", &:posts)
      count_distinct_expression(:unique_uppercased_authors, "UPPER(author_name)", &:posts)
      count_distinct_expression(:unique_topic_lengths, "LENGTH(topic)", &:posts)
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    # Unique pairs: 'John-Tech', 'Jane-Tech', 'John-Sports', 'Bob-Tech'
    expect(@agg[user.id].unique_author_topic_pairs).to eq(4)
    # Unique uppercased authors: 'JOHN', 'JANE', 'BOB'
    expect(@agg[user.id].unique_uppercased_authors).to eq(3)
    # Unique topic lengths: LENGTH('Tech') = 4, LENGTH('Sports') = 6
    expect(@agg[user.id].unique_topic_lengths).to eq(2)
  end

  it "handles string_agg_expression aggregations" do
    # Add columns for testing string aggregation expressions
    Post.connection.add_column Post.table_name, :priority, :integer, default: 1
    Post.connection.add_column Post.table_name, :category_name, :string
    Post.reset_column_information

    user = User.create!(name: "Alice")
    user.posts.create!(title: "Important Post", priority: 1, category_name: "Tech")
    user.posts.create!(title: "Urgent Post", priority: 2, category_name: "Business")
    user.posts.create!(title: "Normal Post", priority: 3, category_name: "General")

    klass = aggregate(User) do
      string_agg_expression(:priority_titles, "priority || ': ' || title", delimiter: " | ", &:posts)
      string_agg_expression(:formatted_categories, "UPPER(category_name)", delimiter: ", ", &:posts)
      string_agg_expression(:title_lengths, "title || ' (' || LENGTH(title) || ' chars)'", delimiter: "; ", &:posts)
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    # Check that the aggregated strings contain expected components
    priority_titles = @agg[user.id].priority_titles.split(" | ").sort
    expect(priority_titles).to match_array(["1: Important Post", "2: Urgent Post", "3: Normal Post"])

    formatted_categories = @agg[user.id].formatted_categories.split(", ").sort
    expect(formatted_categories).to match_array(%w[BUSINESS GENERAL TECH])

    title_lengths = @agg[user.id].title_lengths.split("; ").sort
    expected_lengths = [
      "Important Post (14 chars)",
      "Normal Post (11 chars)",
      "Urgent Post (11 chars)"
    ]
    expect(title_lengths).to match_array(expected_lengths)
  end

  it "handles mixed expression and column aggregations" do
    # Test combining expression-based and column-based aggregations
    Post.connection.add_column Post.table_name, :rating, :decimal, precision: 3, scale: 1, default: 0.0
    Post.connection.add_column Post.table_name, :tags, :string
    Post.reset_column_information

    user = User.create!(name: "Alice")
    user.posts.create!(title: "Post A", views: 100, rating: 4.5, tags: "tech,ruby")
    user.posts.create!(title: "Post B", views: 200, rating: 3.8, tags: "tech,javascript")
    user.posts.create!(title: "Post C", views: 150, rating: 4.2, tags: "business")

    klass = aggregate(User) do
      # Column-based aggregations
      count(:total_posts, &:posts)
      sum(:total_views, :views, &:posts)
      avg(:avg_rating, :rating, &:posts)

      # Expression-based aggregations
      count_expression(:high_rated_posts, "CASE WHEN rating >= 4.0 THEN 1 END", &:posts)
      count_distinct_expression(:unique_tag_counts, "LENGTH(tags) - LENGTH(REPLACE(tags, ',', '')) + 1", &:posts)
      sum_expression(:view_rating_product, "views * rating", &:posts)
      string_agg_expression(:title_rating_summary, "title || ' (' || rating || '★)'", delimiter: " | ", &:posts)
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    expect(@agg[user.id].total_posts).to eq(3)
    expect(@agg[user.id].total_views).to eq(450)
    expect(@agg[user.id].avg_rating.to_f).to be_within(0.01).of(4.17)

    expect(@agg[user.id].high_rated_posts).to eq(2) # Posts A and C have rating >= 4.0
    expect(@agg[user.id].unique_tag_counts).to eq(2) # 2 tags and 1 tag (unique counts)

    # views * rating: (100 * 4.5) + (200 * 3.8) + (150 * 4.2) = 450 + 760 + 630 = 1840
    expect(@agg[user.id].view_rating_product.to_f).to be_within(0.01).of(1840.0)

    summary_parts = @agg[user.id].title_rating_summary.split(" | ").sort
    expected_parts = ["Post A (4.5★)", "Post B (3.8★)", "Post C (4.2★)"]
    expect(summary_parts).to match_array(expected_parts)
  end

  it "handles has_one associations" do
    user1 = User.create!(name: "Alice")
    user2 = User.create!(name: "Bob")

    # Create profiles with different data
    user1.create_profile!(bio: "Ruby developer", verified: true, views: 150)
    user2.create_profile!(bio: "Python enthusiast", verified: false, views: 75)

    klass = aggregate(User) do
      column(:name)
      column(:has_bio) { |user_scope| user_scope.profile.select("bio IS NOT NULL AS has_bio") }
      column(:bio) { |user_scope| user_scope.profile.select(:bio) }
      column(:verified) { |user_scope| user_scope.profile.select(:verified) }
      sum(:profile_views, :views, &:profile)
    end

    expect do
      @results = klass.from(User.all)
    end.to_not exceed_query_limit(1)

    expect(@results[user1.id].name).to eq("Alice")
    expect(@results[user1.id].bio).to eq("Ruby developer")
    expect(@results[user1.id].verified).to eq(1)
    expect(@results[user1.id].profile_views).to eq(150)

    expect(@results[user2.id].name).to eq("Bob")
    expect(@results[user2.id].bio).to eq("Python enthusiast")
    expect(@results[user2.id].verified).to eq(0)
    expect(@results[user2.id].profile_views).to eq(75)
  end

  it "handles complex aggregations with has_one associations" do
    user = User.create!(name: "Charlie", age: 35)
    user.create_profile!(bio: "Full-stack developer", verified: true, views: 200)

    # Create some posts for the user
    user.posts.create!(title: "Post 1", views: 100)
    user.posts.create!(title: "Post 2", views: 50)

    klass = aggregate(User) do
      column(:age)
      column(:bio) { |user_scope| user_scope.profile.select(:bio) }
      sum(:total_views, :views, &:posts)
      sum(:profile_views, :views, &:profile)
    end

    expect do
      @agg = klass.only(user)
    end.to_not exceed_query_limit(1)

    expect(@agg[user.id].age).to eq(35)
    expect(@agg[user.id].bio).to eq("Full-stack developer")
    expect(@agg[user.id].total_views).to eq(150) # Sum of post views
    expect(@agg[user.id].profile_views).to eq(200) # Profile views
  end

  it "handles users with and without has_one associations" do
    user_with_profile = User.create!(name: "Diana")
    user_without_profile = User.create!(name: "Eric")

    user_with_profile.create_profile!(bio: "Architect", verified: true, views: 120)

    klass = aggregate(User) do
      column(:name)
      count(:has_profile, &:profile)
      column(:verified) { |user_scope| user_scope.profile.select(:verified) }
      sum(:profile_views, :views, &:profile)
    end

    expect do
      @results = klass.from(User.all)
    end.to_not exceed_query_limit(1)

    expect(@results[user_with_profile.id].name).to eq("Diana")
    expect(@results[user_with_profile.id].has_profile).to eq(1)
    expect(@results[user_with_profile.id].verified).to eq(1)
    expect(@results[user_with_profile.id].profile_views).to eq(120)

    expect(@results[user_without_profile.id].name).to eq("Eric")
    expect(@results[user_without_profile.id].has_profile).to eq(0)
    expect(@results[user_without_profile.id].verified).to be_nil
    expect(@results[user_without_profile.id].profile_views).to eq(0)
  end

  with_model :Physician do
    table
    model do
      has_many :appointments, dependent: :destroy
      has_many :patients, through: :appointments
    end
  end

  with_model :Patient do
    table do |t|
      t.string :name
    end
    model do
      has_many :appointments, dependent: :destroy
      has_many :physicians, through: :appointments
    end
  end

  with_model :Appointment do
    table do |t|
      t.belongs_to :physician
      t.belongs_to :patient
      t.datetime :appointment_date
    end
    model do
      belongs_to :physician
      belongs_to :patient
    end
  end

  it "handles has_many :through associations" do
    physician1 = Physician.create!
    physician2 = Physician.create!

    patient1 = Patient.create!(name: "Pat1")
    patient2 = Patient.create!(name: "Pat2")
    patient3 = Patient.create!(name: "Pat3")

    # Physician1 sees Pat1, Pat2
    Appointment.create!(physician: physician1, patient: patient1)
    Appointment.create!(physician: physician1, patient: patient2)

    # Physician2 sees Pat2, Pat3
    Appointment.create!(physician: physician2, patient: patient2)
    Appointment.create!(physician: physician2, patient: patient3)

    klass = aggregate(Physician) do
      count(:total_patients, &:patients)
      string_agg(:patient_names, :name, delimiter: ", ", &:patients)
    end

    expect do
      @results = klass.from(Physician.all)
    end.to_not exceed_query_limit(1)

    expect(@results[physician1.id].total_patients).to eq(2)
    physician1_patient_names = @results[physician1.id].patient_names.split(", ").sort
    expect(physician1_patient_names).to match_array(%w[Pat1 Pat2])

    expect(@results[physician2.id].total_patients).to eq(2)
    physician2_patient_names = @results[physician2.id].patient_names.split(", ").sort
    expect(physician2_patient_names).to match_array(%w[Pat2 Pat3])

    # Test with a single physician
    expect do
      @agg_single = klass.only(physician1)
    end.to_not exceed_query_limit(1)
    expect(@agg_single[physician1.id].total_patients).to eq(2)
  end

  it "handles aggregations returning NULL by defaulting to 0 or 0.0" do
    user_no_posts = User.create!(name: "UserWithoutPosts")

    # Post model has 'views' column by default in these tests.
    # We'll use 'views' for both column and expression-based aggregations.
    klass = aggregate(User) do
      sum(:sum_of_views, :views, &:posts)
      avg(:avg_of_views, :views, &:posts)
      min(:min_of_views, :views, &:posts)
      max(:max_of_views, :views, &:posts)

      sum_expression(:sum_expr_views, "views * 2", &:posts)
      avg_expression(:avg_expr_views, "views * 2", &:posts)
      min_expression(:min_expr_views, "views * 2", &:posts)
      max_expression(:max_expr_views, "views * 2", &:posts)
    end

    expect do
      @agg = klass.only(user_no_posts)
    end.to_not exceed_query_limit(1)

    result = @agg[user_no_posts.id]

    expect(result.sum_of_views).to eq(0)
    expect(result.avg_of_views.to_f).to eq(0.0)
    expect(result.min_of_views).to eq(0)
    expect(result.max_of_views).to eq(0)

    expect(result.sum_expr_views).to eq(0)
    expect(result.avg_expr_views.to_f).to eq(0.0)
    expect(result.min_expr_views).to eq(0)
    expect(result.max_expr_views).to eq(0)
  end
end
