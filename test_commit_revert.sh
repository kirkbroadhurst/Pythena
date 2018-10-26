if python -m pytest; then
    git add .
    git commit --allow-empty-message -m ''
else
    git reset --hard
fi
