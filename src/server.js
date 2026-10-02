const express = require('express');
const mongoose = require('mongoose');
const axios = require('axios');
const { Kafka, logLevel } = require('kafkajs');
const cors = require('cors');
const helmet = require('helmet');
const winston = require('winston');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');
const path = require('path');
require('dotenv').config();

// ── Logger ──────────────────────────────────────────────────────────────────
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  defaultMeta: { service: 'order-service' },
  transports: [new winston.transports.Console()]
});

// ── Express App ─────────────────────────────────────────────────────────────
const app = express();
app.use(helmet());
app.use(cors());
app.use(express.json());

// ── Swagger UI ──────────────────────────────────────────────────────────────
const swaggerDoc = YAML.load(path.join(__dirname, 'openapi.yaml'));
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDoc, {
  customCss: '.swagger-ui .topbar { display: none }',
  customSiteTitle: 'Order Service – API Docs'
}));

// ── Mongoose Models ─────────────────────────────────────────────────────────
const basketSchema = new mongoose.Schema(
  {
    customerId: String,
    items: [
      {
        cakeId:   String,
        name:     String,
        price:    Number,
        imageUrl: String,
        weight:   String,
        quantity: Number
      }
    ],
    total: Number
  },
  { timestamps: true }
);

const orderSchema = new mongoose.Schema(
  {
    customerId:    String,
    customerEmail: String,
    items:         Array,
    total:         Number,
    status:        String
  },
  { timestamps: true }
);

const Basket = mongoose.model('Basket', basketSchema);
const Order  = mongoose.model('Order', orderSchema);

// ── Kafka Producer ──────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: 'order-service',
  brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(','),
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 1000, retries: 10 }
});

const producer = kafka.producer();
const TOPIC = 'order-events';

// ── Helpers ─────────────────────────────────────────────────────────────────
const getCustomerId = (req) => req.headers['x-customer-id'] || 'demo-customer';
const calcTotal = (items) => items.reduce((sum, i) => sum + i.price * i.quantity, 0);

// ── Routes ──────────────────────────────────────────────────────────────────

// Health check
app.get('/health', (_req, res) => {
  res.json({ service: 'order-service', status: 'ok' });
});

// Get basket (with image auto-population if missing)
app.get('/api/basket', async (req, res, next) => {
  try {
    const customerId = getCustomerId(req);
    let basket = await Basket.findOne({ customerId });
    if (!basket) {
      return res.json({ customerId, items: [], total: 0 });
    }

    // Ensure any items missing imageUrl get populated from catalog
    const catalogUrl = process.env.CATALOG_URL || 'http://catalog-service:3001';
    let updated = false;
    for (const item of basket.items) {
      if (!item.imageUrl) {
        try {
          const { data: cake } = await axios.get(`${catalogUrl}/api/cakes/${item.cakeId}`);
          item.imageUrl = cake.imageUrl;
          item.weight = cake.weight || '500g';
          updated = true;
        } catch {}
      }
    }
    if (updated) await basket.save();

    res.json(basket);
  } catch (err) {
    next(err);
  }
});

// Add item to basket
app.post('/api/basket/items', async (req, res, next) => {
  try {
    const { cakeId, quantity = 1 } = req.body;

    if (!cakeId || quantity < 1) {
      return res.status(400).json({
        error: { message: 'cakeId and positive quantity are required' }
      });
    }

    // Fetch cake details from catalog service
    const catalogUrl = process.env.CATALOG_URL || 'http://catalog-service:3001';
    const { data: cake } = await axios.get(`${catalogUrl}/api/cakes/${cakeId}`);

    const customerId = getCustomerId(req);
    let basket = await Basket.findOne({ customerId });
    if (!basket) {
      basket = new Basket({ customerId, items: [] });
    }

    const existing = basket.items.find((x) => x.cakeId === cakeId);
    if (existing) {
      existing.quantity += Number(quantity);
      if (!existing.imageUrl && cake.imageUrl) existing.imageUrl = cake.imageUrl;
    } else {
      basket.items.push({
        cakeId,
        name: cake.name,
        price: cake.price,
        imageUrl: cake.imageUrl,
        weight: cake.weight || '500g',
        quantity: Number(quantity)
      });
    }

    basket.total = calcTotal(basket.items);
    await basket.save();

    logger.info('Item added to basket', { customerId, cakeId, quantity });
    res.status(201).json(basket);
  } catch (err) {
    next(err);
  }
});

// Update item quantity in basket
app.patch('/api/basket/items/:cakeId', async (req, res, next) => {
  try {
    const customerId = getCustomerId(req);
    const basket = await Basket.findOne({ customerId });
    const item = basket?.items.find((x) => x.cakeId === req.params.cakeId);

    if (!item) {
      return res.status(404).json({ error: { message: 'Item not found in basket' } });
    }

    item.quantity = Math.max(1, Number(req.body.quantity));
    basket.total = calcTotal(basket.items);
    await basket.save();

    logger.info('Basket item updated', { customerId, cakeId: req.params.cakeId, quantity: item.quantity });
    res.json(basket);
  } catch (err) {
    next(err);
  }
});

// Remove item from basket
app.delete('/api/basket/items/:cakeId', async (req, res, next) => {
  try {
    const customerId = getCustomerId(req);
    const basket = await Basket.findOne({ customerId });

    if (!basket) {
      return res.json({ items: [], total: 0 });
    }

    basket.items = basket.items.filter((x) => x.cakeId !== req.params.cakeId);
    basket.total = calcTotal(basket.items);
    await basket.save();

    logger.info('Item removed from basket', { customerId, cakeId: req.params.cakeId });
    res.json(basket);
  } catch (err) {
    next(err);
  }
});

// Checkout – check stock, deduct inventory, create order & publish Kafka event
app.post('/api/orders/checkout', async (req, res, next) => {
  try {
    const customerId = getCustomerId(req);
    const basket = await Basket.findOne({ customerId });

    if (!basket?.items.length) {
      return res.status(400).json({ error: { message: 'Basket is empty' } });
    }
    if (!req.body.customerEmail) {
      return res.status(400).json({ error: { message: 'customerEmail is required' } });
    }

    const catalogUrl = process.env.CATALOG_URL || 'http://catalog-service:3001';

    // Step 1: Check stock availability for all items
    for (const item of basket.items) {
      try {
        const { data: cake } = await axios.get(`${catalogUrl}/api/cakes/${item.cakeId}`);
        if (cake.quantity < item.quantity) {
          return res.status(400).json({
            error: {
              code: 'INSUFFICIENT_STOCK',
              message: `"${item.name}" only has ${cake.quantity} in stock (requested ${item.quantity})`
            }
          });
        }
      } catch (err) {
        return res.status(400).json({
          error: { code: 'STOCK_CHECK_FAILED', message: `Could not verify stock for "${item.name}"` }
        });
      }
    }

    // Step 2: Deduct stock for all items
    for (const item of basket.items) {
      try {
        await axios.patch(`${catalogUrl}/api/cakes/${item.cakeId}/stock`, {
          delta: -item.quantity
        });
        logger.info('Stock deducted', { cakeId: item.cakeId, qty: item.quantity });
      } catch (err) {
        logger.error('Stock deduction failed', { cakeId: item.cakeId, error: err.message });
      }
    }

    // Step 3: Create the order
    const order = await Order.create({
      customerId,
      customerEmail: req.body.customerEmail,
      items: basket.items,
      total: basket.total,
      status: 'CONFIRMED'
    });

    // Clear the basket
    await Basket.deleteOne({ _id: basket._id });

    // Publish order-completed event to Kafka
    const event = {
      eventId:    String(order._id),
      eventType:  'ORDER_COMPLETED',
      occurredAt: new Date().toISOString(),
      data: {
        orderId:       String(order._id),
        customerId:    order.customerId,
        customerEmail: order.customerEmail,
        items:         order.items,
        total:         order.total
      }
    };

    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key:   String(order._id),
          value: JSON.stringify(event)
        }
      ]
    });

    logger.info('Order completed & event published', {
      orderId: order._id,
      total: order.total
    });

    res.status(201).json(order);
  } catch (err) {
    next(err);
  }
});

// Get order by ID
app.get('/api/orders/:id', async (req, res, next) => {
  try {
    const order = await Order.findById(req.params.id);
    if (!order) {
      return res.status(404).json({ error: { message: 'Order not found' } });
    }
    res.json(order);
  } catch (err) {
    next(err);
  }
});

// List orders for a customer
app.get('/api/orders', async (req, res, next) => {
  try {
    const customerId = getCustomerId(req);
    const orders = await Order.find({ customerId }).sort({ createdAt: -1 });
    res.json(orders);
  } catch (err) {
    next(err);
  }
});

// ── Global Error Handler ────────────────────────────────────────────────────
app.use((err, _req, res, _next) => {
  logger.error('Unhandled error', { error: err.message, stack: err.stack });
  res.status(500).json({
    error: { code: 'ORDER_ERROR', message: err.message }
  });
});

// ── Start Server ────────────────────────────────────────────────────────────
async function start() {
  // Connect to MongoDB
  await mongoose.connect(process.env.MONGO_URI);
  logger.info('Connected to MongoDB');

  // Connect Kafka producer
  await producer.connect();
  logger.info('Kafka producer connected');

  const PORT = process.env.PORT || 3002;
  app.listen(PORT, () => logger.info(`order-service ready on port ${PORT}`));
}

start().catch((err) => {
  logger.error('Failed to start order-service', { error: err.message });
  process.exit(1);
});
