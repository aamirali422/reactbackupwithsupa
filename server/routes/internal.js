// routes/internal.js (or wherever you define the route)
router.get('/tickets', async (req, res) => {
  try {
    const q = (req.query.q || '').trim();
    const limit = Math.max(1, Math.min(1000, Number(req.query.limit) || 100));

    // ... your DB query ...
  } catch (err) {
    console.error('GET /api/internal/tickets failed:', err); // <— important
    res.status(500).json({ error: 'Internal error', detail: String(err?.message || err) });
  }
});
