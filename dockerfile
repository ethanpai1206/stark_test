
# 第一階段：構建基礎映像
# FROM nvidia/cuda:12.6.2-cudnn-devel-ubuntu22.04
FROM behance/docker-base:5.2.0-ubuntu-22.04
# 設置環境變數
ENV UBUNTU_HOME=/home/ubuntu \
    PASSWORD="22601576" \
    DEBIAN_FRONTEND=noninteractive \
    TZ=Asia\Taipei \
    LANG=zh_TW.UTF8 \
    LC_ALL=zh_TW.UTF8 \
    LANGUAGE=zh_TW.UTF8

# 配置時區和本地化設置
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    apt-get update && \
    apt-get install -y --no-install-recommends tzdata locales && \
    dpkg-reconfigure --frontend noninteractive tzdata && \
    locale-gen zh_TW.UTF-8 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 安裝必要的依賴包
RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential cmake pkg-config unzip libgtk2.0-dev \
    wget curl ca-certificates libcurl4-openssl-dev libssl-dev \
    libavcodec-dev libavformat-dev libswscale-dev libtbbmalloc2 libtbb-dev \
    libharfbuzz-dev libfreetype-dev libpq-dev\
    libaio-dev libgoogle-perftools-dev libopenblas-dev tini supervisor openssh-server \
    clang-format clang-tidy lcov libtool m4 autoconf automake \
    libgstreamer-plugins-base1.0-dev libgstreamer1.0-dev libgstrtspserver-1.0-dev libx11-dev \
    libjpeg-turbo8-dev libpng-dev libtiff-dev libdc1394-dev nasm && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 安裝 Python 3.10
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.10 python3.10-dev python3.10-distutils python3-gst-1.0 python3-pybind11 && \
    wget https://bootstrap.pypa.io/get-pip.py && \
    python3.10 get-pip.py && \
    ln -s /usr/bin/python3.10 /usr/local/bin/python && \
    rm get-pip.py

# 創建用戶 ubuntu
RUN useradd -m -s /bin/bash ubuntu

# 創建目錄和文件，確保目標存在
RUN mkdir -p ${UBUNTU_HOME} && \
    touch ${UBUNTU_HOME}/.bashrc

# 配置用戶環境
RUN echo 'export LANGUAGE="zh_TW.UTF-8"' >> ${UBUNTU_HOME}/.bashrc && \
    echo 'export LANG="zh_TW.UTF-8"' >> ${UBUNTU_HOME}/.bashrc && \
    echo 'export LC_ALL="zh_TW.UTF-8"' >> ${UBUNTU_HOME}/.bashrc && \
    chown ubuntu:ubuntu -R ${UBUNTU_HOME}

# 設置工作目錄
WORKDIR $UBUNTU_HOME

# 添加 ubuntu 用戶到 sudo 群組
RUN usermod -aG sudo ubuntu

# 切換到 ubuntu 用戶
USER ubuntu

# 創建專案目錄
RUN mkdir -p ${UBUNTU_HOME}/workspace/stark_test

# 先複製 requirements.txt 並安裝依賴包
COPY --chown=ubuntu:ubuntu requirements.txt ${UBUNTU_HOME}/workspace/stark_test/
RUN cd ${UBUNTU_HOME}/workspace/stark_test && \
    python -m pip install --user --upgrade pip && \
    python -m pip install --user -r requirements.txt

# 複製整個專案到容器內
COPY --chown=ubuntu:ubuntu . ${UBUNTU_HOME}/workspace/stark_test/

# 設置工作目錄為專案目錄
WORKDIR ${UBUNTU_HOME}/workspace/stark_test

CMD ["/bin/bash"]