if python -m pytest; then
    git add .
    git commit --allow-empty-message -m '$1'
    git push
else
    git reset --hard
fi
