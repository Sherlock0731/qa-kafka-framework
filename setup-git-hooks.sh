#!/bin/bash

# Скрипт для установки git hooks для защиты от случайного коммита сертификатов

echo "🔐 Настройка Git Hooks для безопасности..."

# Создание pre-commit hook
cat > .git/hooks/pre-commit << 'PREHOOK'
#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🔍 Проверка безопасности перед коммитом..."

# Проверка на сертификаты
CERT_FILES=$(git diff --cached --name-only | grep -E '\.(jks|p12|pem|key|crt|cer|pfx)$' || true)
if [ ! -z "$CERT_FILES" ]; then
    echo ""
    echo "❌ КРИТИЧЕСКАЯ ОШИБКА: Попытка закоммитить сертификаты!"
    echo "=================================================="
    echo "Эти файлы содержат приватные ключи и НЕ должны быть в Git:"
    echo "$CERT_FILES"
    echo ""
    echo "Добавьте их в .gitignore и удалите из staging:"
    echo "  git reset HEAD <file>"
    exit 1
fi

# Проверка на .env файлы
if git diff --cached --name-only | grep -qE '^\.env$|/\.env$'; then
    echo "❌ ОШИБКА: Попытка закоммитить .env файл!"
    echo ""
    echo "Файлы .env содержат секреты и НЕ должны быть в Git!"
    echo "Используйте .env.example вместо этого"
    exit 1
fi

# Проверка на пароли в коде
if git diff --cached | grep -qiE 'password["\']?\s*[:=]\s*["\'][^"'\'']+["\']'; then
    echo "⚠️  ПРЕДУПРЕЖДЕНИЕ: Обнаружен возможный пароль в коде!"
    echo "Проверьте файлы на наличие захардкоженных паролей"
    git diff --cached | grep -i password
fi

echo "✅ Проверка безопасности пройдена"
exit 0
