// Import necessary libraries
const express = require('express');
const mongoose = require('mongoose');

// Create an Express application
const app = express();

// Middleware to parse JSON requests
app.use(express.json());

// Connect to MongoDB
mongoose.connect('mongodb://localhost:27017/hi', {
    useNewUrlParser: true,
    useUnifiedTopology: true
})
    .then(() => console.log('Connected to MongoDB'))
    .catch(err => console.error('Error connecting to MongoDB:', err));

// Define a schema for the document
const Schema = mongoose.Schema;
const productSchema = new Schema({
    product: String,
    price: Number,
    quantity: Number,
    date: Date
});
const userSchema = new Schema({
    name: String,
    age: Number,
    gender: String,
    friends: [String],
    posts: [
        {
            text: String,
            date: Date
        }
    ]
});
const ratingSchema = new Schema({
    user: String,
    score: Number
});

const movieSchema = new Schema({
    title: String,
    release_year: Number,
    genre: String,
    director: String,
    actors: [String],
    ratings: [ratingSchema]
});
const orderSchema = new Schema({
    order_id: String,
    customer_id: String,
    product: String,
    price: Number,
    quantity: Number,
    date: Date
});
const orderSchema5 = new mongoose.Schema({
    order_id: String,
    customer_id: String,
    total_amount: Number,
    order_date: Date
});
const eventSchema = new mongoose.Schema({
    event_date: { type: Date, required: true },
    user_id: { type: Schema.Types.ObjectId, required: true },
    event_type: { type: String, required: true }
});
const saleSchema = new Schema({
    sale_date: { type: Date, required: true },
    product_id: { type: Schema.Types.ObjectId, required: true },
    quantity: { type: Number, required: true },
    price: { type: Number, required: true }
});

const Sale = mongoose.model('Sale', saleSchema);

const Event = mongoose.model('Event', eventSchema);

const Order5 = mongoose.model('Order5', orderSchema5);
// Create the Order model
const Order = mongoose.model('Order', orderSchema);

const Movie = mongoose.model('Movie', movieSchema);

// Create a model from the schema
const User = mongoose.model('User', userSchema);


// Create a model from the schema
const Product = mongoose.model('Product', productSchema);

/*------------------1-----------------------
You are given a collection of sales documents with the following fields: product, price, quantity, and date. Write an aggregation pipeline that returns the total revenue, total quantity sold, and average price per unit for each product, sorted by total revenue in descending order.
{
  "_id": ObjectId("606d7f464d66a20b5a08c412"),
  "product": "Product A",
  "price": 30,
  "quantity": 20,
  "date": ISODate("2022-03-20T10:30:00Z")
}

*/
app.post('/products', async (req, res) => {
    try {
        const { product, price, quantity, date } = req.body;
        const newProduct = new Product({
            product,
            price,
            quantity,
            date
        });
        await newProduct.save();
        res.status(201).json({ message: 'Product created successfully' });
    } catch (err) {
        console.error('Error creating product:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});
// API endpoint for the aggregation pipeline that returns the total revenue, total quantity sold, and average price per unit for each product, sorted by total revenue in descending order.
app.get('/products/stats', async (req, res) => {
    try {
        const stats = await Product.aggregate([
            {
                $group: {
                    _id: "$product",
                    totalRevenue: { $sum: { $multiply: ["$price", "$quantity"] } },
                    totalQuantitySold: { $sum: "$quantity" },
                    averagePricePerUnit: { $avg: "$price" }
                }
            },
            {
                $sort: { totalRevenue: -1 }
            }
        ]);
        res.json(stats);
    } catch (err) {
        console.error('Error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

// API endpoint to create a user document
app.post('/users', async (req, res) => {
    try {
        const { name, age, gender, friends, posts } = req.body;
        const newUser = new User({
            name,
            age,
            gender,
            friends,
            posts
        });
        await newUser.save();
        res.status(201).json({ message: 'User created successfully' });
    } catch (err) {
        console.error('Error creating user:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});
// API endpoint to get average age and gender distribution of user's friends
app.get('/users/friend-stats', async (req, res) => {
    try {
        const result = await User.aggregate([
            {
                $group: {
                    _id: null,
                    averageAge: { $avg: "$age" }
                }
            }
        ]);
        res.json({ averageAge: result[0].averageAge });
    } catch (err) {
        console.error('Error:', err);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

/** -----3---------
 * You are given a collection of movie documents with the following fields: title, release_year, genre, director, actors (an array of actor names), and ratings (an array of rating documents with fields user and score). Write an aggregation pipeline that returns the average rating for each genre, sorted by average rating in descending order.
// Example document for a movies collection
{
  "_id": ObjectId("606d7f464d66a20b5a08c414"),
  "title": "Movie A",
  "release_year": 2022,
  "genre": "Action",
  "director": "John Smith",
  "actors": ["Tom Cruise", "Emily Blunt"],
  "ratings": [
    {"user": "User A", "score": 4},
    {"user": "User B", "score": 5},
    {"user": "User C", "score": 3}
  ]
}
 * 
 */
app.post('/movies', async (req, res) => {
    try {
        const movie = new Movie(req.body);
        const result = await movie.save();
        res.status(201).send(result);
    } catch (error) {
        res.status(500).send({ error: 'Failed to insert movie document', message: error.message });
    }
});
// Route to get average ratings by genre
app.get('/average-ratings', async (req, res) => {
    try {
        const pipeline = [
            {
                "$unwind": "$ratings"
            },
            {
                "$group": {
                    "_id": "$genre",
                    "averageRating": { "$avg": "$ratings.score" }
                }
            },
            {
                "$sort": { "averageRating": -1 }
            }
        ];
        const result = await Movie.aggregate(pipeline).exec();
        res.status(200).send(result);
    } catch (error) {
        res.status(500).send({ error: 'Failed to retrieve average ratings', message: error.message });
    }
});

/***------------------------------------4-----------------------------------
 * 
 * You are given a collection of order documents with the following fields: order_id, customer_id, product, price, quantity, and date. Write an aggregation pipeline that returns the total revenue and number of orders for each customer, sorted by total revenue in descending order. Additionally, include the customer's name by looking it up in a separate collection of customer documents with fields customer_id and name.
// Example document for an orders collection
{
  "_id": ObjectId("606d7f464d66a20b5a08c415"),
  "order_id": "12345",
  "customer_id": "ABC123",
  "product": "Product A",
  "price": 30,
  "quantity": 20,
  "date": ISODate("2022-03-20T10:30:00Z")
}
*/
app.post('/orders', async (req, res) => {
    try {
        const order = new Order(req.body);
        const result = await order.save();
        res.status(201).send(result);
    } catch (error) {
        res.status(500).send({ error: 'Failed to insert order document', message: error.message });
    }
});

// Route to get customer revenue information
app.get('/customer-revenue', async (req, res) => {
    try {
        const pipeline = [
            {
                "$group": {
                    "_id": "$customer_id",
                    "totalRevenue": { "$sum": { "$multiply": ["$price", "$quantity"] } },
                    "numberOfOrders": { "$sum": 1 }
                }
            },
            {
                "$sort": { "totalRevenue": -1 }
            }
        ];
        const result = await Order.aggregate(pipeline).exec();
        res.status(200).send(result);
    } catch (error) {
        res.status(500).send({ error: 'Failed to retrieve customer revenue information', message: error.message });
    }
});

/**---------------------5-----------------------------------
 * You have a MongoDB collection named "orders" that contains documents with the following fields: "_id", "order_date", "customer_id", and "total_amount". You want to aggregate the total amount of orders placed by each customer for a given date range.
// Example document for an orders collection
{
  "_id": ObjectId("606d7f464d66a20b5a08c415"),
  "order_id": "12345",
  "customer_id": "ABC123",
  "total_amount": 300,
  "order_date": ISODate("2022-03-20")
}
 * 
 * **/
app.post('/order5', async (req, res) => {
    try {
        const newOrder = await Order5.create(req.body);
        res.status(201).json(newOrder);
    } catch (err) {
        res.status(400).json({ message: err.message });
    }
});
app.post('/order5-agg', async (req, res) => {
    try {
        const { startDate, endDate } = req.body;
        const result = await Order5.aggregate([
            {
                $match: {
                    order_date: {
                        $gte: new Date(startDate),  // Start date of the range
                        $lte: new Date(endDate)   // End date of the range
                    }
                }
            },
            {
                $group: {
                    _id: "$customer_id",
                    totalAmount: { $sum: "$total_amount" }
                }
            },
            {
                $sort: { totalAmount: -1 }  // Optional: Sort by total amount in descending order
            }
        ])

        res.json(result);
    } catch (err) {
        res.status(400).json({ message: err.message });
    }
});
/**----------------------6--------------------------------------
 * You have a MongoDB collection named "events" that contains documents with the following fields: "_id", "event_date", "user_id", and "event_type". You want to aggregate the number of events of each type that were triggered by each user for a given date range.
{
  "_id": ObjectId("61fcf3eb925f8d65bced9a51"),
  "event_date": ISODate("2022-01-03T04:58:23.000Z"),
  "user_id": ObjectId("61fcf3eb925f8d65bced9a50"),
  "event_type": "click"
}
 * 
 * 
 * **/
app.post('/add-event', async (req, res) => {

    
    try {
        const newEvent = await Event.create(req.body);


        res.status(201).json(newEvent);
    } catch (err) {
        console.error('Insertion error', err);
        res.status(500).send('Internal server error');
    }
});
app.post('/aggregate-events', async (req, res) => {
    const { startDate, endDate } = req.body;

    
    try {
        const pipeline = [
            {
                $match: {
                    event_date: {
                        $gte: new Date(startDate),
                        $lte: new Date(endDate)
                    }
                }
            },
            {
                $group: {
                    _id: {
                        user_id: '$user_id',
                        event_type: '$event_type'
                    },
                    count: { $sum: 1 }
                }
            },
            {
                $group: {
                    _id: '$_id.user_id',
                    events: {
                        $push: {
                            event_type: '$_id.event_type',
                            count: '$count'
                        }
                    }
                }
            },
            {
                $project: {
                    _id: 0,
                    user_id: '$_id',
                    events: 1
                }
            }
        ];

        const result = await Event.aggregate(pipeline).exec();

        res.json(result);
    } catch (err) {
        console.error('Aggregation error', err);
        res.status(500).send('Internal server error');
    }
});


/*
-----------------------------7-------------------------------------------
You have a MongoDB collection named "sales" that contains documents with the following fields: "_id", "sale_date", "product_id", "quantity", and "price". You want to aggregate the total revenue generated by each product for a given date range.
{
  "_id": ObjectId("61fcf541925f8d65bced9a53"),
  "sale_date": ISODate("2022-01-03T06:10:23.000Z"),
  "product_id": ObjectId("61fcf541925f8d65bced9a52"),
  "quantity": 2,
  "price": 49.99
}
**/
app.post('/add-sale', async (req, res) => {
    const { sale_date, product_id, quantity, price } = req.body;

    if (!sale_date || !product_id || !quantity || !price) {
        return res.status(400).send('sale_date, product_id, quantity, and price are required');
    }

    try {
       
        const result = await Sale.create(req.body);

        res.status(201).json(result);
    } catch (err) {
        console.error('Insertion error', err);
        res.status(500).send('Internal server error');
    }
});
app.post('/total-revenue', async (req, res) => {
    const { startDate, endDate } = req.body;

    if (!startDate || !endDate) {
        return res.status(400).send('startDate and endDate are required');
    }

    try {
        const pipeline = [
            {
                $match: {
                    sale_date: {
                        $gte: new Date(startDate),
                        $lte: new Date(endDate)
                    }
                }
            },
            {
                $group: {
                    _id: '$product_id',
                    totalRevenue: { $sum: { $multiply: ['$quantity', '$price'] } }
                }
            },
            {
                $lookup: {
                    from: 'products', // Replace with the name of the products collection
                    localField: '_id',
                    foreignField: '_id',
                    as: 'product'
                }
            },
            {
                $unwind: '$product'
            },
            {
                $project: {
                    _id: 0,
                    product_id: '$_id',
                    product_name: '$product.name', // Assuming there's a 'name' field in the products collection
                    totalRevenue: 1
                }
            }
        ];

        const result = await Sale.aggregate(pipeline).exec();

        res.json(result);
    } catch (err) {
        console.error('Aggregation error', err);
        res.status(500).send('Internal server error');
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
