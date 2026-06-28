from fpdf import FPDF
from fpdf.enums import XPos, YPos
import datetime
import csv

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import os

class PDFReport(FPDF):
    def __init__(self, title, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.title = title
        # Пробуем использовать красивые шрифты, если они доступны (их мы скачивали в нашу папку ttf/)
        regular_font_path = os.path.join('ttf', 'DejaVuSans.ttf')
        bold_font_path = os.path.join('ttf', 'DejaVuSans-Bold.ttf')

        if os.path.exists(regular_font_path) and os.path.exists(bold_font_path):
            self.add_font('DejaVu', '', regular_font_path)
            self.add_font('DejaVu', 'B', bold_font_path)

            self.font_family = 'DejaVu'
        else:
            # Если шрифты не найдены, используем стандартный
            self.font_family = 'Arial'

    def header(self):
        """Создаёт шапку для каждой страницы"""
        self.set_font(self.font_family, 'B', 15)
        self.cell(0, 10, self.title, border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
        self.set_font(self.font_family, '', 8)
        self.cell(0, 5, f'Дата генерации: {datetime.date.today().strftime("%d.%m.%Y")}', 0, 1, 'C')
        self.ln(10)

    def footer(self):
        """Добавляет номера страниц в подвале"""
        self.set_y(-15)
        self.set_font(self.font_family, 'B', 8)
        self.cell(0, 10, f'Страница {self.page_no()}', border=0, new_x=XPos.RIGHT, new_y=YPos.TOP, align='C')

    def chapter_title(self, title):
        """Создаёт заголовок раздела"""
        self.set_font(self.font_family, 'B', 12)
        self.cell(0, 10, title, border=0, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='L')
        self.ln(5)

    def chapter_body(self, body):
        """Добавляет основной текст"""
        self.set_font(self.font_family, '', 10)
        self.multi_cell(0, 5, body)
        self.ln()

    def add_image(self, image_path, image_width = 200):
        """Добавляет секцию с изображением и статистикой"""
        # Центрируем изображение на странице
        image_width = image_width
        page_width = self.w - 2 * self.l_margin
        x_position = (page_width - image_width) / 2 + self.l_margin
        self.image(image_path, x=x_position, y=None, w=image_width)
        self.ln(5)
        self.set_font(self.font_family, '', 10) 

    def add_table_from_csv(self, csv_path):
        """Добавляет таблицу из CSV-файла в PDF.
        """

        with open(csv_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            data = list(reader)

        self.set_font(self.font_family, '', 10)
        col_width = self.w / len(data[0]) - 10
        row_height = self.font_size * 1.5

        for row in data:
            for item in row:
                self.cell(col_width, row_height, str(item), border=1)
            self.ln(row_height)

class PDFCreater:
    def __init__(self,
                title,
                report_out_path,
                train_path,
                valid_path,
                test_path,
                folder_path_info,
                save_path_data_info,
                metrics_path,
                graph_metrics_path,
                focs_path_images,
                yolo_path_images
            ):
        self.title = title
        self.report_out_path = report_out_path
        self.train_path = train_path
        self.valid_path = valid_path
        self.test_path = test_path
        self.folder_path_info = folder_path_info
        self.save_path_data_info = save_path_data_info
        self.metrics_path = metrics_path
        self.graph_metrics_path = graph_metrics_path
        self.focs_path_images = focs_path_images
        self.yolo_path_images = yolo_path_images
    
    def create_report(self):
        # Сборка PDF-отчёта
        pdf = PDFReport(self.title)
        pdf.add_page()

        # Общая сводка
        pdf.chapter_title("1. Общая сводка по датасетам")
        train_image = self.get_all_image(self.train_path)
        valid_image = self.get_all_image(self.valid_path)
        test_image = self.get_all_image(self.test_path)

        summary_text = (
            f"КОЛИЧЕСТВО ПРОАНАЛИЗИРОВАННЫХ ИЗОБРАЖЕНИЙ:\n"
            f"Количество изображений в тренировочном датасете: {len(train_image)}\n"
            f"Количество изображений в валидационном датасете: {len(valid_image)}\n"
            f"Количество изображений в тестовом датасете: {len(test_image)}\n\n"
            f"ВИЗУАЛИЗАЦИЯ ДАТАСЕТОВ:\n"
        )
        pdf.chapter_body(summary_text)
        self.get_graph_data(self.folder_path_info, self.save_path_data_info)
        pdf.add_image(self.save_path_data_info)

        # Сравнение моделей
        pdf.add_page()
        pdf.chapter_title("2. Сравнение обученных моделей")
        pdf.chapter_body("ИТОГОВЫЕ МЕТРИКИ МОДЕЛЕЙ НА ТЕСТОВОМ ДАТАСЕТЕ:")
        pdf.add_table_from_csv(self.metrics_path)
        pdf.chapter_body("\n\nИЗМЕНЕНИЕ МЕТРИК НА ВАЛИДАЦИИ ПРИ ОБУЧЕНИИ:")
        pdf.add_image(self.graph_metrics_path)


        pdf.add_page()
        pdf.chapter_title("3. Визуализация работы моделей")
        fcos_images = [os.path.join(self.focs_path_images, f) for f in self.get_example_list(self.focs_path_images)]
        yolo_images = [os.path.join(self.yolo_path_images, f) for f in self.get_example_list(self.yolo_path_images)]

        for fcos, yolo in zip(fcos_images, yolo_images):
            pdf.chapter_body("ПРИМЕР РАБОТЫ МОДЕЛИ FCOS:")    
            pdf.add_image(fcos, 100)
            
            pdf.chapter_body("ПРЕМЕР РАБОТЫ МОДЕЛИ YOLO:")    
            pdf.add_image(yolo, 100)
            pdf.add_page()


        pdf.output(self.report_out_path)

    def get_all_image(self, dir: str) -> str:
        image_ext = ('.jpg', '.jpeg', '.png')

        list_files = os.listdir(dir) 
        all_images = [f for f in list_files
                    if os.path.isfile(os.path.join(dir, f)) 
                    and f.lower().endswith(image_ext)]
        return all_images

    def get_graph_data(self, folder_path: str = "artifacts/datasets_info", save_path: str = 'tmp/datasets_info.png'):
        image_files = [f for f in os.listdir(folder_path) if f.endswith('.png')]
        image_files.sort()

        n_files = min(9, len(image_files))
        image_files = image_files[:n_files]

        fig, axes = plt.subplots(3, 3, figsize=(15, 10))
        plt.subplots_adjust(left=0, right=1, top=1, bottom=0, wspace=0, hspace=0)

        for idx, ax in enumerate(axes.flat):
            if idx < n_files:
                img_path = os.path.join(folder_path, image_files[idx])
                img = mpimg.imread(img_path)
                ax.imshow(img)
                ax.set_axis_off()
                
                for spine in ax.spines.values():
                    spine.set_visible(False)
            else:
                ax.set_visible(False)

        plt.savefig(save_path, pad_inches=0, facecolor='black')
        plt.close(fig)

    def get_example_list(self, dir: str, count_example: int=3):
        image_ext = ('.jpg', '.jpeg', '.png')

        list_files = os.listdir(dir) 
        example_list = [f for f in list_files
                        if os.path.isfile(os.path.join(dir, f)) 
                        and f.lower().endswith(image_ext) and 'example' in f]
        return example_list[: count_example]
        