# Shape Up Workflow (Solo + AI)

Quick reference for running this project with Shape Up methodology.

## The Six-Week Cycle (Adapted for Solo)

```
┌──────────────────────────────────────────────────────┐
│  COOL-DOWN (2-3 days)                                │
│  • Fix bugs from last cycle                          │
│  • Refactor code                                     │
│  • Shape new pitches                                 │
│  • Explore ideas without committing                  │
│  • REST (seriously, rest)                            │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│  BETTING TABLE (30 minutes)                          │
│  • Review shaped pitches in /pitches                 │
│  • Ask: "What's the most valuable problem?"          │
│  • Pick ONE pitch (or pick nothing)                  │
│  • Set appetite (1 week, 2 weeks, etc.)              │
│  • Update BETTING_TABLE.md                           │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│  BUILD CYCLE (1-2 weeks)                             │
│  • Work on the bet                                   │
│  • Update HILL_CHART.md daily                        │
│  • At 50% time: Check hill position                  │
│  • Cut scope if stuck on left side                   │
│  • Ship or kill at deadline                          │
└──────────────────────────────────────────────────────┘
                        ↓
                   (repeat)
```

## Daily Workflow (During Build Cycle)

### Morning Check-In (5 min)

1. Open `HILL_CHART.md`
2. Ask yourself: "Where am I on the hill?"
   - 0-50% = Figuring things out
   - 50-100% = Making it happen
3. Update position and notes
4. Check if scope cuts needed

### Work Session

- Focus on ONE thing at a time
- Follow the pitch's rabbit holes (what NOT to do)
- Cut features early if getting stuck
- Ship working code, not perfect code

### End of Day (5 min)

1. Update `HILL_CHART.md` with progress
2. Note learnings or blockers
3. Plan tomorrow's focus

### 50% Time Checkpoint (CRITICAL)

**If you're at 50% of time budget:**

```
Ask: "Am I past the hill (50% position)?"

If YES (> 50% position):
  └─ Keep going, you're on track!

If NO (< 50% position):
  ├─ EMERGENCY: Cut all COULD HAVEs immediately
  ├─ Cut half the NICE TO HAVEs
  ├─ Simplify MUST HAVEs to bare minimum
  └─ Re-assess if bet is viable
```

**Never extend the deadline. Ship what works or kill it.**

## Shaping Process (During Cool-Down)

### When You Have an Idea

1. **Don't start building** - Shape first!
2. Write a pitch using `/pitches/TEMPLATE.md`
3. Key questions to answer:
   - What problem does this solve? (Why now?)
   - How much time is it worth? (Appetite)
   - What's the rough approach? (Fat marker sketch)
   - What should I NOT do? (Rabbit holes)
   - What's explicitly out? (No-gos)
   - What does done look like? (Concrete example)

### Shaping Quality Check

A pitch is well-shaped if:
- ✅ Problem is clear and specific
- ✅ Appetite is set (time budget)
- ✅ Solution is sketched (not detailed)
- ✅ Rabbit holes are marked
- ✅ Scope line exists (must/nice/could)
- ✅ You're excited to work on it

A pitch needs more shaping if:
- ❌ Too vague ("improve performance")
- ❌ No time budget set
- ❌ Over-specified (detailed implementation)
- ❌ No clear "done" criteria
- ❌ Feels like an obligation, not opportunity

## Betting Decision (After Cool-Down)

### The Four Questions

Before betting on a pitch, ask:

1. **Is it shaped?**
   - Does it have clear problem, appetite, solution, rabbit holes?

2. **Is it the right bet?**
   - Is this the most valuable thing right now?
   - Will I learn something important?

3. **Am I excited?**
   - Do I actually want to work on this?
   - Or does it feel like a chore?

4. **Can I cut scope?**
   - Is the scope flexible enough to ship on time?
   - Are there clear must-haves vs nice-to-haves?

**If any answer is "no" → Don't bet on it**

### Valid Bets

- **Bet on a shaped pitch** (the usual choice)
- **Bet on nothing** (extended cool-down)
  - Valid when: tired, uncertain, need exploration time
  - Not a failure - protects against burnout
- **Bet on two small pitches** (rare, only if < 1 week each)

### Invalid Bets

- ❌ Bet on multiple big things at once
- ❌ Bet on unshaped work ("we'll figure it out")
- ❌ Bet because you "should" (no real excitement)
- ❌ Bet on something too big for appetite

## Scope Management

### The Scope Line (Define in Pitch)

```
CUT FIRST:
──────────────────
Features that add polish
Nice-to-haves that aren't critical
Optimizations

TRY TO INCLUDE:
───────────────
Enhancements that improve UX
Features that add value

MUST HAVE:
══════════════════
Core functionality
Bare minimum for success
```

### When to Cut Scope

**Immediately cut when**:
- At 50% time, still on left side of hill
- Spending > 1 day stuck on same problem
- Found unexpected complexity
- Original estimate was wrong

**How to cut**:
1. Cut all "could haves" first
2. Cut "nice to haves" next
3. Simplify "must haves" to bare minimum
4. Ship working version or kill the bet

**Never**:
- Extend the deadline
- Work overtime to "make it fit"
- Skip testing to save time
- Ship broken code

## Anti-Patterns to Avoid

### ❌ The Backlog Trap
- Treating pitches like a todo list
- Feeling obligated to build every pitch
- Planning multiple cycles ahead

**Instead**: Pitches are options, not commitments

### ❌ Scope Creep
- Adding features mid-cycle
- "Just one more thing" syndrome
- Perfectionism

**Instead**: Stick to the pitch. New ideas → new pitches

### ❌ Time Extension
- "Almost done, just need 2 more days"
- Extending cycles to finish everything
- Working overtime

**Instead**: Ship what works or kill it. Re-pitch if valuable.

### ❌ Skipping Cool-Down
- Jumping straight to next cycle
- "We're on a roll, let's keep going"
- Burning out

**Instead**: Mandatory cool-down. Rest prevents burnout.

### ❌ Over-Planning
- Detailed specs before building
- Too much upfront design
- Analysis paralysis

**Instead**: Fat marker sketches. Figure out details while building.

## Files Reference

- `BETTING_TABLE.md` - Current cycle decision
- `HILL_CHART.md` - Daily progress tracking
- `pitches/` - Library of shaped pitches (options)
- `pitches/TEMPLATE.md` - Pitch format
- `pitches/README.md` - Pitching guidelines

## Working with AI (Claude)

### During Shaping
- "Help me shape a pitch for [problem]"
- "What are the rabbit holes in this idea?"
- "Is this pitch well-shaped?"

### During Betting
- "Review these pitches and help me decide"
- "What's the risk of betting on X?"
- "Should I bet on this or take cool-down?"

### During Building
- "Where am I on the hill?"
- "Should I cut scope now?"
- "This feature is taking too long - what can I cut?"

### Emergency Scope Cuts
- "I'm at 50% time and still figuring things out - help me cut scope"
- "What's the bare minimum to ship?"
- "Can we simplify this must-have?"

## Quick Commands

```bash
# Start cool-down (after shipping)
# Just rest! Fix bugs. Explore. Shape pitches.

# Review pitches
ls -la pitches/

# Make a bet
vim BETTING_TABLE.md  # Update with decision

# Daily progress
vim HILL_CHART.md  # Update position

# Shape new pitch
cp pitches/TEMPLATE.md pitches/PITCH_[name].md
vim pitches/PITCH_[name].md

# Emergency scope cut
# Open your pitch, review scope line, cut features
```

## Mantras

- **"Fixed time, variable scope"** - Ship on time, cut features
- **"Done is better than perfect"** - Ship working code
- **"Bet on one thing"** - Focus wins
- **"Pitches, not backlog"** - Options, not obligations
- **"Cool-down is mandatory"** - Rest prevents burnout
- **"Ship or kill"** - No extensions, no excuses

---

## Success Metrics

You're doing Shape Up right if:

- ✅ Shipping on deadline consistently
- ✅ Not feeling burned out
- ✅ Cutting scope confidently when needed
- ✅ Excited about the work
- ✅ Learning from each cycle
- ✅ Saying "no" to non-bets

You're doing it wrong if:

- ❌ Constantly extending deadlines
- ❌ Working overtime regularly
- ❌ Building features out of obligation
- ❌ Feeling stressed about "the backlog"
- ❌ Skipping cool-downs
- ❌ Can't cut scope when needed

**Remember**: Shape Up is about sustainable pace and shipping. If you're burning out or missing deadlines, you're not following it correctly.

