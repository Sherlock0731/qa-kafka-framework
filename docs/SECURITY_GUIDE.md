# Руководство по безопасности Kafka сертификатов

## Проблема

Файлы `kafka.keystore.p12` и `kafka.truststore.jks` содержат приватные ключи и сертификаты, которые:
- ❌ **НИКОГДА** не должны попадать в Git репозиторий
- ❌ **НИКОГДА** не должны быть публично доступны
- ✅ **ДОЛЖНЫ** быть доступны для GitHub Actions
- ✅ **ДОЛЖНЫ** быть доступны для локальной разработки

## Решение

Используем комбинацию `.gitignore`, GitHub Secrets и Base64 кодирования.

---

## Защита в Git репозитории

### Обновите .gitignore

Убедитесь, что в `.gitignore` есть:

```gitignore
# === Kafka Сертификаты (КРИТИЧНО!) ===
*.jks
*.p12
*.pem
*.key
*.crt
*.cer
kafka_key/
kafka-certs/
certificates/

# === Файлы с секретами ===
.env
*.env
!.env.example
local.properties
*-local.properties

# === Пароли и токены ===
*password*
*secret*
*token*
*.credentials
```

### Проверка текущего репозитория

Если файлы уже были закоммичены, удалите их из истории:

```bash
# ВНИМАНИЕ: Это перепишет историю Git!

# Удалить файл из истории
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch kafka_key/kafka.keystore.p12" \
  --prune-empty --tag-name-filter cat -- --all

git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch kafka_key/kafka.truststore.jks" \
  --prune-empty --tag-name-filter cat -- --all

# Или используйте более современный инструмент
git filter-repo --invert-paths --path kafka_key/

# Force push (будьте осторожны!)
git push origin --force --all
```

Альтернативный безопасный способ (рекомендуется):
```bash
# Удалить файлы из tracking
git rm --cached kafka_key/*.jks
git rm --cached kafka_key/*.p12

# Закоммитить изменения
git commit -m "Remove sensitive certificates from tracking"

# Добавить в .gitignore
echo "kafka_key/" >> .gitignore
git add .gitignore
git commit -m "Add kafka certificates to .gitignore"
```

---

## Настройка для локальной разработки

### Вариант A: Локальная директория (Рекомендуется)

**1. Создайте директорию вне репозитория:**
```bash
# Linux/macOS
mkdir -p ~/secure/kafka-certs
cp kafka.keystore.p12 ~/secure/kafka-certs/
cp kafka.truststore.jks ~/secure/kafka-certs/
chmod 600 ~/secure/kafka-certs/*

# Windows
mkdir C:\secure\kafka-certs
copy kafka.keystore.p12 C:\secure\kafka-certs\
copy kafka.truststore.jks C:\secure\kafka-certs\
```

**2. Обновите `src/main/resources/config/local.properties`:**
```properties
# Linux/macOS
kafka.ssl.truststore.location=/home/ваш_пользователь/secure/kafka-certs/kafka.truststore.jks
kafka.ssl.keystore.location=/home/ваш_пользователь/secure/kafka-certs/kafka.keystore.p12

# Windows
kafka.ssl.truststore.location=C:\\secure\\kafka-certs\\kafka.truststore.jks
kafka.ssl.keystore.location=C:\\secure\\kafka-certs\\kafka.keystore.p12
```

**3. Установите переменные окружения для паролей:**
```bash
# Linux/macOS - добавьте в ~/.bashrc или ~/.zshrc
export KAFKA_SSL_TRUSTSTORE_PASSWORD="ваш_пароль"
export KAFKA_SSL_KEYSTORE_PASSWORD="ваш_пароль"

# Windows - добавьте в системные переменные
setx KAFKA_SSL_TRUSTSTORE_PASSWORD "ваш_пароль"
setx KAFKA_SSL_KEYSTORE_PASSWORD "ваш_пароль"
```

### Вариант B: Использование .env файла (для Docker)

**1. Создайте `.env` файл в корне проекта:**
```bash
cp .env.example .env
```

**2. Отредактируйте `.env`:**
```bash
# Пароли
KAFKA_SSL_TRUSTSTORE_PASSWORD=ваш_реальный_пароль
KAFKA_SSL_KEYSTORE_PASSWORD=ваш_реальный_пароль
KAFKA_REST_API_PASSWORD=ваш_api_пароль
KAFKA_SCHEMA_REGISTRY_PASSWORD=ваш_registry_пароль

# Путь к сертификатам (опционально)
KAFKA_CERTS_PATH=/путь/к/вашим/сертификатам
```

**3. Убедитесь, что `.env` в `.gitignore`:**
```bash
grep "^.env$" .gitignore || echo ".env" >> .gitignore
```

---

## Настройка GitHub Actions

### Шаг 1: Конвертация сертификатов в Base64

```bash
# Linux/macOS
base64 -i kafka.truststore.jks -o truststore.txt
base64 -i kafka.keystore.p12 -o keystore.txt

# Windows PowerShell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("C:\kafka_key\kafka.truststore.jks")) | Out-File truststore.txt
[Convert]::ToBase64String([IO.File]::ReadAllBytes("C:\kafka_key\kafka.keystore.p12")) | Out-File keystore.txt

# Скопируйте содержимое файлов truststore.txt и keystore.txt
cat truststore.txt
cat keystore.txt

# ВАЖНО: Удалите временные файлы!
rm truststore.txt keystore.txt
```

### Шаг 2: Добавление GitHub Secrets

1. Откройте ваш репозиторий на GitHub
2. Settings → Secrets and variables → Actions
3. Нажмите "New repository secret"
4. Добавьте следующие секреты:

| Имя секрета | Значение |
|-------------|----------|
| `KAFKA_TRUSTSTORE_BASE64` | Содержимое truststore.txt |
| `KAFKA_KEYSTORE_BASE64` | Содержимое keystore.txt |
| `KAFKA_SSL_TRUSTSTORE_PASSWORD` | Пароль от truststore |
| `KAFKA_SSL_KEYSTORE_PASSWORD` | Пароль от keystore |
| `KAFKA_REST_API_PASSWORD` | Пароль для REST API |
| `KAFKA_SCHEMA_REGISTRY_PASSWORD` | Пароль для Schema Registry |

### Шаг 3: Обновление GitHub Actions Workflow

Workflow уже настроен правильно в `.github/workflows/test-all.yml`:

```yaml
- name: Setup Kafka SSL certificates
  run: |
    mkdir -p kafka_key
    echo "${{ secrets.KAFKA_TRUSTSTORE_BASE64 }}" | base64 -d > kafka_key/kafka.truststore.jks
    echo "${{ secrets.KAFKA_KEYSTORE_BASE64 }}" | base64 -d > kafka_key/kafka.keystore.p12
    chmod 600 kafka_key/*

- name: Run tests
  env:
    KAFKA_SSL_TRUSTSTORE_PASSWORD: ${{ secrets.KAFKA_SSL_TRUSTSTORE_PASSWORD }}
    KAFKA_SSL_KEYSTORE_PASSWORD: ${{ secrets.KAFKA_SSL_KEYSTORE_PASSWORD }}
  run: mvn clean test -Pparallel -Dthread.count=4
```

---

## Проверка безопасности

### Чеклист перед коммитом

```bash
# 1. Проверьте .gitignore
cat .gitignore | grep -E "\.jks|\.p12|\.env"

# 2. Проверьте, что файлы не отслеживаются
git status | grep -E "\.jks|\.p12|\.env"

# 3. Проверьте историю Git (не должно быть совпадений)
git log --all --full-history -- "*.jks" "*.p12" "*.env"

# 4. Проверьте staged файлы
git diff --cached --name-only | grep -E "\.jks|\.p12|\.env"

# 5. Поиск секретов в коде
grep -r "password.*=" src/ | grep -v "Properties\|Config\|EXAMPLE"
```

### Автоматическая проверка с git hooks

Создайте `.git/hooks/pre-commit`:

```bash
#!/bin/bash

# Проверка на случайный коммит сертификатов
if git diff --cached --name-only | grep -qE '\.jks$|\.p12$|\.pem$|\.key$'; then
    echo "❌ ОШИБКА: Попытка закоммитить сертификаты!"
    echo "Файлы:"
    git diff --cached --name-only | grep -E '\.jks$|\.p12$|\.pem$|\.key$'
    echo ""
    echo "Эти файлы содержат приватные ключи и НЕ должны быть в Git!"
    exit 1
fi

# Проверка на .env файлы
if git diff --cached --name-only | grep -qE '^\.env$'; then
    echo "❌ ОШИБКА: Попытка закоммитить .env файл!"
    echo "Используйте .env.example вместо этого"
    exit 1
fi

echo "✅ Проверка безопасности пройдена"
exit 0
```

Сделайте hook исполняемым:
```bash
chmod +x .git/hooks/pre-commit
```

---

## Безопасность в Docker

### docker-compose.yml правильно настроен:

```yaml
services:
  kafka-tests:
    volumes:
      # Монтирование локальных сертификатов (только для чтения)
      - ${KAFKA_CERTS_PATH:-./kafka_key}:/app/kafka_key:ro
    environment:
      # Пароли из .env файла (не из docker-compose.yml!)
      - KAFKA_SSL_TRUSTSTORE_PASSWORD=${KAFKA_SSL_TRUSTSTORE_PASSWORD}
      - KAFKA_SSL_KEYSTORE_PASSWORD=${KAFKA_SSL_KEYSTORE_PASSWORD}
```

### .dockerignore правильно настроен:

```dockerignore
# Сертификаты (будут монтированы как volume)
*.jks
*.p12
*.pem
*.key
kafka_key/
```

---

## Ротация сертификатов

Если сертификаты скомпрометированы:

### 1. Локально
```bash
# Удалите старые
rm ~/secure/kafka-certs/*

# Получите новые от администратора Kafka
# Скопируйте новые
cp new-kafka.truststore.jks ~/secure/kafka-certs/
cp new-kafka.keystore.p12 ~/secure/kafka-certs/
```

### 2. В GitHub Secrets
1. Конвертируйте новые сертификаты в Base64
2. Обновите секреты в GitHub: Settings → Secrets → Actions
3. Нажмите "Update" для каждого секрета

### 3. В команде
Уведомите всех разработчиков:
```
ВАЖНО: Kafka сертификаты обновлены!

1. Получите новые сертификаты из [защищенного хранилища]
2. Замените файлы в ~/secure/kafka-certs/
3. Обновите пароли в переменных окружения
4. GitHub Actions уже обновлены
```

---

## Альтернативные решения

### Hashicorp Vault (для enterprise)

```bash
# Сохранить в Vault
vault kv put secret/kafka \
  truststore=@kafka.truststore.jks \
  keystore=@kafka.keystore.p12 \
  truststore_password="xxx" \
  keystore_password="yyy"

# Получить в CI/CD
vault kv get -field=truststore secret/kafka > kafka.truststore.jks
```

### AWS Secrets Manager

```bash
# Сохранить
aws secretsmanager create-secret \
  --name kafka-truststore \
  --secret-binary fileb://kafka.truststore.jks

# Получить в CI/CD
aws secretsmanager get-secret-value \
  --secret-id kafka-truststore \
  --query SecretBinary --output text | base64 -d > kafka.truststore.jks
```

### Azure Key Vault

```bash
# Сохранить
az keyvault secret set \
  --vault-name myvault \
  --name kafka-truststore \
  --file kafka.truststore.jks

# Получить в CI/CD
az keyvault secret download \
  --vault-name myvault \
  --name kafka-truststore \
  --file kafka.truststore.jks
```

---

## Мониторинг и аудит

### GitHub Audit Log
Регулярно проверяйте:
- Settings → Security → Audit log
- Фильтр: "action:secret.*"

### Alerts на случайные коммиты
Используйте GitHub Secret Scanning (автоматически для public repos).

Для private repos:
- Settings → Security → Code security and analysis
- Enable "Secret scanning"

---

## Итоговый чеклист безопасности

- [ ] Файлы *.jks, *.p12 в .gitignore
- [ ] Файл .env в .gitignore
- [ ] Сертификаты удалены из Git истории (если были)
- [ ] Локальные сертификаты в безопасной директории (вне проекта)
- [ ] Переменные окружения настроены локально
- [ ] GitHub Secrets добавлены (6 секретов)
- [ ] GitHub Actions workflow протестирован
- [ ] Pre-commit hook установлен
- [ ] .dockerignore исключает сертификаты
- [ ] docker-compose.yml использует переменные окружения
- [ ] Команда уведомлена о политике безопасности
- [ ] План ротации сертификатов документирован

---

## Что делать если сертификаты утекли

1. **Немедленно:**
   - Отзовите скомпрометированные сертификаты
   - Смените пароли
   - Получите новые сертификаты

2. **Очистите Git историю:**
   ```bash
   git filter-repo --invert-paths --path kafka_key/
   git push origin --force --all
   ```

3. **Обновите:**
   - GitHub Secrets
   - Локальные копии у всех разработчиков
   - CI/CD пайплайны

4. **Аудит:**
   - Проверьте логи доступа к Kafka
   - Проверьте GitHub Audit Log
   - Задокументируйте инцидент

---

## Дополнительные ресурсы

- [GitHub Encrypted Secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
- [Git Filter-Repo](https://github.com/newren/git-filter-repo)
- [OWASP Secrets Management](https://owasp.org/www-community/vulnerabilities/Secrets_Management)

---

**Версия:** 1.0.0  
**Дата:** 2026-01-13
