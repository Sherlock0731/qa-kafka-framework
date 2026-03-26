# Быстрая настройка GitHub Secrets

## Что нужно сделать

Добавить 9 секретов в GitHub для безопасного использования Kafka сертификатов и Aiven API в CI/CD.

---

## Пошаговая инструкция

### Шаг 1: Конвертация сертификатов в Base64

**Linux/macOS:**
```bash
# Truststore
base64 -i kafka_key/kafka.truststore.jks | tr -d '\n' > truststore_base64.txt

# Keystore  
base64 -i kafka_key/kafka.keystore.p12 | tr -d '\n' > keystore_base64.txt

# Посмотрите результат
cat truststore_base64.txt
cat keystore_base64.txt
```

**Windows PowerShell:**
```powershell
# Truststore
$truststore = [Convert]::ToBase64String([IO.File]::ReadAllBytes("kafka_key\kafka.truststore.jks"))
$truststore | Out-File -NoNewline truststore_base64.txt

# Keystore
$keystore = [Convert]::ToBase64String([IO.File]::ReadAllBytes("kafka_key\kafka.keystore.p12"))
$keystore | Out-File -NoNewline keystore_base64.txt

# Посмотрите результат
Get-Content truststore_base64.txt
Get-Content keystore_base64.txt
```

**ВАЖНО:** Скопируйте содержимое файлов truststore_base64.txt и keystore_base64.txt

---

### Шаг 2: Добавление секретов в GitHub

1. Откройте ваш репозиторий на GitHub
2. Перейдите: **Settings** → **Secrets and variables** → **Actions**
3. Нажмите кнопку **"New repository secret"**
4. Добавьте каждый секрет по очереди:

---

#### Секрет 1: KAFKA_TRUSTSTORE_BASE64

- **Name:** `KAFKA_TRUSTSTORE_BASE64`
- **Secret:** Вставьте содержимое файла `truststore_base64.txt` (вся строка без пробелов)
- Нажмите **"Add secret"**

---

#### Секрет 2: KAFKA_KEYSTORE_BASE64

- **Name:** `KAFKA_KEYSTORE_BASE64`
- **Secret:** Вставьте содержимое файла `keystore_base64.txt` (вся строка без пробелов)
- Нажмите **"Add secret"**

---

#### Секрет 3: KAFKA_SSL_TRUSTSTORE_PASSWORD

- **Name:** `KAFKA_SSL_TRUSTSTORE_PASSWORD`
- **Secret:** Введите пароль от truststore (обычный текст)
- Нажмите **"Add secret"**

---

#### Секрет 4: KAFKA_SSL_KEYSTORE_PASSWORD

- **Name:** `KAFKA_SSL_KEYSTORE_PASSWORD`
- **Secret:** Введите пароль от keystore (обычный текст)
- Нажмите **"Add secret"**

---

#### Секрет 5: KAFKA_REST_API_PASSWORD

- **Name:** `KAFKA_REST_API_PASSWORD`
- **Secret:** Введите пароль для Kafka REST API
- Нажмите **"Add secret"**

---

#### Секрет 6: KAFKA_SCHEMA_REGISTRY_PASSWORD

- **Name:** `KAFKA_SCHEMA_REGISTRY_PASSWORD`
- **Secret:** Введите пароль для Schema Registry
- Нажмите **"Add secret"**

---

#### Секрет 7: AIVEN_API_TOKEN

- **Name:** `AIVEN_API_TOKEN`
- **Secret:** Введите токен API от Aiven для автоматической очистки топиков
- Нажмите **"Add secret"**

**Как получить токен:**
1. Войдите в Aiven Console: https://console.aiven.io/
2. Перейдите в User Information → Authentication
3. Нажмите "Generate token"
4. Скопируйте токен и добавьте в GitHub Secrets

---

#### Секрет 8: AIVEN_PROJECT_NAME

- **Name:** `AIVEN_PROJECT_NAME`
- **Secret:** Введите название вашего проекта в Aiven
- Нажмите **"Add secret"**

---

#### Секрет 9: AIVEN_SERVICE_NAME

- **Name:** `AIVEN_SERVICE_NAME`
- **Secret:** Введите название вашего Kafka сервиса в Aiven
- Нажмите **"Add secret"**

---

### Шаг 3: Проверка

После добавления всех секретов вы должны увидеть:

```
Repository secrets (9)

• KAFKA_TRUSTSTORE_BASE64          Updated now by you
• KAFKA_KEYSTORE_BASE64            Updated now by you
• KAFKA_SSL_TRUSTSTORE_PASSWORD    Updated now by you
• KAFKA_SSL_KEYSTORE_PASSWORD      Updated now by you
• KAFKA_REST_API_PASSWORD          Updated now by you
• KAFKA_SCHEMA_REGISTRY_PASSWORD   Updated now by you
• AIVEN_API_TOKEN                  Updated now by you
• AIVEN_PROJECT_NAME               Updated now by you
• AIVEN_SERVICE_NAME               Updated now by you
```

---

### Шаг 4: Удаление временных файлов

**КРИТИЧНО:** Удалите временные файлы с Base64:

```bash
# Linux/macOS
rm -f truststore_base64.txt keystore_base64.txt

# Windows
del truststore_base64.txt
del keystore_base64.txt
```

---

### Шаг 5: Тест GitHub Actions

1. Сделайте любое изменение в коде
2. Закоммитьте и запушьте:
   ```bash
   git add .
   git commit -m "Test GitHub Actions with secrets"
   git push
   ```
3. Перейдите в **Actions** на GitHub
4. Проверьте, что workflow выполнился успешно

---

## Проверка секретов в workflow

Секреты используются в `.github/workflows/test-all.yml`:

```yaml
- name: Setup Kafka SSL certificates
  run: |
    mkdir -p kafka_key
    # Декодирование Base64 обратно в файлы
    echo "${{ secrets.KAFKA_TRUSTSTORE_BASE64 }}" | base64 -d > kafka_key/kafka.truststore.jks
    echo "${{ secrets.KAFKA_KEYSTORE_BASE64 }}" | base64 -d > kafka_key/kafka.keystore.p12
    chmod 600 kafka_key/*

- name: Run tests
  env:
    # Пароли как переменные окружения
    KAFKA_SSL_TRUSTSTORE_PASSWORD: ${{ secrets.KAFKA_SSL_TRUSTSTORE_PASSWORD }}
    KAFKA_SSL_KEYSTORE_PASSWORD: ${{ secrets.KAFKA_SSL_KEYSTORE_PASSWORD }}
    KAFKA_REST_API_PASSWORD: ${{ secrets.KAFKA_REST_API_PASSWORD }}
    KAFKA_SCHEMA_REGISTRY_PASSWORD: ${{ secrets.KAFKA_SCHEMA_REGISTRY_PASSWORD }}
    # Aiven API для автоматической очистки топиков
    AIVEN_API_TOKEN: ${{ secrets.AIVEN_API_TOKEN }}
    AIVEN_PROJECT_NAME: ${{ secrets.AIVEN_PROJECT_NAME }}
    AIVEN_SERVICE_NAME: ${{ secrets.AIVEN_SERVICE_NAME }}
  run: mvn clean test
```

---

## Безопасность

### ✅ Что ПРАВИЛЬНО:
- Сертификаты закодированы в Base64 и хранятся в GitHub Secrets
- Пароли хранятся в GitHub Secrets (не в коде!)
- Временные файлы удалены
- Сертификаты в .gitignore

### ❌ Что НЕПРАВИЛЬНО:
- Коммитить *.jks или *.p12 файлы
- Хранить пароли в .env и коммитить его
- Хардкодить пароли в коде
- Оставлять truststore_base64.txt на диске

---

## Обновление секретов

Если нужно обновить сертификаты:

1. Получите новые сертификаты
2. Конвертируйте в Base64 (Шаг 1)
3. На GitHub: Settings → Secrets → Actions
4. Нажмите на имя секрета (например, KAFKA_TRUSTSTORE_BASE64)
5. Нажмите **"Update secret"**
6. Вставьте новое значение
7. Нажмите **"Update secret"**

---

## Troubleshooting

### Ошибка: "base64: invalid input"

**Причина:** В Base64 строке есть переносы строк или пробелы

**Решение:** Используйте `tr -d '\n'` (Linux/macOS) или `-NoNewline` (Windows)

### Ошибка: "FileNotFoundException" в CI

**Причина:** Секреты не добавлены или имеют неправильное имя

**Решение:** 
1. Проверьте точное имя секрета (учитывается регистр!)
2. Убедитесь, что секрет добавлен в repository secrets, а не environment secrets

### Ошибка: "Invalid keystore format"

**Причина:** Файл некорректно декодирован из Base64

**Решение:**
1. Проверьте, что Base64 строка полная (без обрезки)
2. Пересоздайте Base64 с флагом `-i` и `-o` или с `tr -d '\n'`

---

## Дополнительно

- [GitHub Encrypted Secrets Documentation](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
- [Полное руководство по безопасности](docs/SECURITY_GUIDE.md)
- [.gitignore](../.gitignore) - Проверьте, что сертификаты исключены

---

**Версия:** 1.2.0  
**Дата:** 2026-01-26  
**Изменения:** Добавлена поддержка Aiven API для автоматической очистки топиков
