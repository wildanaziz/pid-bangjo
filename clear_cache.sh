#!/bin/bash
# Script to clear Python cache and verify the fix

echo "=============================================="
echo "Clearing Python Cache"
echo "=============================================="

# Remove __pycache__ directories
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
echo "✓ Removed __pycache__ directories"

# Remove .pyc files
find . -type f -name "*.pyc" -delete 2>/dev/null
echo "✓ Removed .pyc files"

# Remove .pyo files
find . -type f -name "*.pyo" -delete 2>/dev/null
echo "✓ Removed .pyo files"

echo ""
echo "=============================================="
echo "Verifying Fix in main_integrated.py"
echo "=============================================="

# Check if the fix is applied
if grep -q "cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), color, 2)" main_integrated.py; then
    echo "✓ Line 199: cv2.rectangle is CORRECT"
    echo "  Pattern: cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), color, 2)"
else
    echo "✗ Line 199: cv2.rectangle has an error"
    echo "  Showing actual line:"
    grep -n "cv2.rectangle" main_integrated.py | head -1
fi

echo ""
echo "=============================================="
echo "Cache cleared and verification complete!"
echo "=============================================="
echo ""
echo "You can now run: python main_integrated.py"
