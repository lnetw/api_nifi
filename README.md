## Класс Api Nifi для автоматизации процессов

### Установка и начало работы
1. Перенесите репозиторий себе на локальную машину c помощью git
**https://github.com/lnetw/api_nifi.git**
2. Создайте файл **config.py** в котором буду лежать ваши учетная запись (username) и пароль (password) для подключения к NiFi, сам файл должен содержать 2 строки:
   
**username = 'username'**

**password = 'password'**

3. Установите необходимые для работы библиотеки из файла **requirements.txt**

4. Воспользуйтесь функционалом класса

### Список доступных методов класса

1. `ApiEngine(base_url, log, time, username, password)` - Объявление класса
2. `access()`- проверка есть ли доступ (201 значит что все хорошо)
3. `get_ignore_group(id_parent_process_group, ignore_name)`- Функция для нахождения неиспользуемых групп процессоров
4. `get_process_group(id_parent_process_group, ignore_name='')`- Функция для нахождения списка всех групп процессов в группе
5. `get_processor_in_group(id_parent_process_group, path_process_group)`- Функция для нахождения списка всех процессоров в группе"
6. `preOrder(id_parent_process_group, result_dict, ignore_name='')`- Функция для рекурсивного обхода групп процессов
7. `get_dict_processor_group(id_parent_process_group, ignore_name='')`- Функция для извлечения словаря - дерево, из рекурсивного обхода группы процессов
8. `get_abs_path(target_dict, key)`- Функция для нахождения пути до процесса"
9. `extract_processor(etalon, dict_group, list_temp)`- Функция сбора процессов в единый список
10. `get_all_processor(id_parent_process_group, ignore_name='')`- Функция загрузки списка процессов в единый DataFrame
11. `change_state(id_proc, state, version)`- Функция изменения состояния процессоров
12. `start_processor(df_change_state)`- Функция запуска процессоров
13. `stop_processor(df_change_state)`-Функция остановки процессоров
14. `disabel_processor(df_change_state)`- Функция отключения процессоров

### Используемые переменные

1. **base_url** - url расположения NiFi
2. **log** - Логировать ли выполнение переключения режимов процессоров
3. **time** - Время задержки между выполнением операций
4. **username** - Логин для в систему
5. **password** - Пароль для входа в систему
