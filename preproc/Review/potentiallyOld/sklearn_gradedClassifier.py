# Myra describing new information about grading confidence 
'''  Myra describing new information about grading confidence 
You said:
ah! I just remembered - we also see goal directed navigation when a participant is moving to their blue cylinders at the start of every block 
- which might also involve a bit of visual scanning right before movement. This actually makes me think: Walking towards a visible target (a chest or a blue cylinder) 
become cases of 100% confidence goal directed navigation. Meanwhile, we are going to have lower degrees of confidence in navigation in the pin dropping phase that are 
exemplified in uncertain/meandering states. I think it might make sense (after ruling out that a phase isn't 'Idle') to grade the walking behavior? However, this would 
be a continuous variable when I was aiming to make a categorical one. What do you think given this extra information? 
'''

# Chat GPT Core Insights 
'''
ChatGPT said:
This is an awesome refinement — and you're intuitively converging on a really elegant modeling strategy.

🧠 Core Insight:
You're identifying a spectrum of goal-directed behavior — from fully certain, visible-target navigation to ambiguous or memory-driven, uncertain paths.

This breaks the binary mold of:

“Goal-directed” vs “Not”

into:

Graded intentionality — some movements are clearly goal-driven, some are ambiguous, some are meandering.
'''

# ✅ Proposed Adjustment: From Classification to Scoring
'''
Instead of forcing discrete classes, score walking periods on a continuous scale representing confidence of goal-directedness.

⚖️ Options:
🔹 Option A: Hybrid Model (Best of Both Worlds)
First, classify between:

"Idle"

"Walking"

Then, for walking segments, regress a goal-directedness score (e.g. 0 to 1)

This maintains interpretability while capturing nuance:

Score ~1.0 = moving to visible object (e.g. blue cylinder or chest)

Score ~0.5 = wandering during pin dropping

Score ~0.0 = barely moving or looping uncertainly

🔹 Option B: Discretize Later
Train a regressor on known graded examples (e.g. annotated walks to cylinders/chests vs. memory-search walking), then:

Post-process the score into bins:

[0.8–1.0] → "GoalDirectedCertain"

[0.4–0.8] → "UncertainGoal"

[0.0–0.4] → "Meandering"

This allows tuning flexibility after model training.
'''

# 🔬 Feature Strategy:
'''
To grade confidence, combine these signals:


Feature	Why It Helps
Path straightness	High = confident movement
Speed consistency	More consistent = intentional
Time since round started	Longer = more uncertainty
Gaze entropy	High = scanning/searching
Head rotation rate	High = searching
Distance to known goal location	Close and moving quickly = confident
Is goal visible? (if available)	If yes, likely high confidence
🛠 Model Choice:
For continuous scoring: Gradient Boosting Regressor or Neural Net

For hybrid (classification → regression): Sklearn pipeline or a 2-stage model
'''

# 📈 Labeling Strategy:
'''
Use your visible-target navigation periods (walk to blue cylinder, to visible chest) as gold-standard high-confidence positive labels.

Then use:

Post-block idling = 0

Mid-block wandering = [0.2–0.5] (based on heuristics or manual review)

Mid-block drop-paths = [0.7–1.0] depending on path clarity

You can even manually grade a few and use them for calibration.
'''

# ✅ Summary Recommendation:
'''
YES to scoring — it’s a better match for your data and goals.

Use discrete classes only if needed for downstream tasks.

Your walking data is perfect for a confidence-of-goal-directedness model — especially with gaze/head movement in the mix.

Would you like a starter template for a hybrid classifier + regressor pipeline, or a feature extractor to get rolling?

You said:
yes please! Is both a good idea? If not, I think I'll opt towards the hybrid classifier + regressor pipeline version 
ChatGPT said:
Excellent choice — a hybrid classifier + regressor pipeline is absolutely a smart way to go. It’s interpretable, flexible, and allows you to:

Filter out idle segments confidently, and

Score walking segments for their level of goal-directedness.

This structure mirrors how humans analyze movement: first asking "Are they even walking?", then "How purposefully are they walking?"
'''

# ✅ Pipeline Overview
'''
Step 1: Walking Segment Classification
Detect:

"Idle" (not moving / low activity)

"Walking" (active movement)

Model: Classifier

Step 2: Goal-Directedness Regression
Only on "Walking" segments, predict a goal-directedness score ∈ [0.0, 1.0]

Model: Regressor

🛠 Starter Pipeline Template (Sklearn-based)
'''

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, mean_squared_error

# Step 1: Feature engineering placeholder
def extract_features(segment):
    return {
        "mean_speed": np.mean(segment['speed']),
        "speed_std": np.std(segment['speed']),
        "path_straightness": compute_path_straightness(segment),
        "head_rotation_rate": np.mean(segment['head_rot_speed']),
        "gaze_entropy": compute_entropy(segment['gaze_directions']),
        "duration": segment['timestamp'].iloc[-1] - segment['timestamp'].iloc[0]
    }

# Placeholder helpers
def compute_path_straightness(segment):
    start = segment[['x', 'y']].iloc[0]
    end = segment[['x', 'y']].iloc[-1]
    straight_dist = np.linalg.norm(end - start)
    path_dist = np.sum(np.sqrt(np.diff(segment['x'])**2 + np.diff(segment['y'])**2))
    return straight_dist / path_dist if path_dist > 0 else 0

def compute_entropy(values):
    hist, _ = np.histogram(values, bins=10, range=(0, 360), density=True)
    hist = hist[hist > 0]
    return -np.sum(hist * np.log(hist))

# Step 2: Build dataset
# Assume segments is a list of DataFrames, each a movement segment
feature_list = []
labels_step1 = []  # "Idle" or "Walking"
labels_step2 = []  # Goal-directedness score (float) — only for Walking

for segment in segments:
    features = extract_features(segment)
    feature_list.append(features)

    is_idle = segment['speed'].mean() < 0.05  # example threshold
    labels_step1.append(0 if is_idle else 1)  # 0 = Idle, 1 = Walking

    if not is_idle:
        labels_step2.append(segment.goal_directedness_score)  # You provide this

X = pd.DataFrame(feature_list)
y_cls = np.array(labels_step1)
y_reg = np.array(labels_step2)

# Step 3: Train classifier
X_train, X_test, y_train, y_test = train_test_split(X, y_cls, test_size=0.2, random_state=42)
clf = RandomForestClassifier()
clf.fit(X_train, y_train)
print("Classifier report:\n", classification_report(y_test, clf.predict(X_test)))

# Step 4: Train regressor on 'walking' only
X_walk = X[y_cls == 1]
y_walk = y_reg
X_train_r, X_test_r, y_train_r, y_test_r = train_test_split(X_walk, y_walk, test_size=0.2, random_state=42)
reg = GradientBoostingRegressor()
reg.fit(X_train_r, y_train_r)
print("Regressor MSE:", mean_squared_error(y_test_r, reg.predict(X_test_r)))

# 🔧 What You’ll Need to Plug In
'''
segments: A list of DataFrames, each with a walking or idle period

goal_directedness_score: For training, either hand-annotated or heuristically generated

Features like speed, head_rot_speed, and gaze_directions from your tracking system

Optional Extension:
You can wrap this into a function like tag_movement_segments() that outputs:

{
  "SegmentID": ...,
  "IsWalking": True,
  "GoalDirectednessScore": 0.87,
  "GoalDirectednessCategory": "High"
}
Let me know if you want help:

Processing your full tracking logs into segments

Creating an annotation tool for grading segments manually

Converting this pipeline into a deployable tagger

Would you like to explore a feature extractor next?