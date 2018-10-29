if [ -z "$1" ]; then
    echo "No commit message - use \"\" if genuinely no commit message"
    exit 1
fi

if python -m pytest ; then
    msg=$@
    git add .
    git commit --allow-empty-message -m "$msg"
    git push
else
    git reset --hard
fi
