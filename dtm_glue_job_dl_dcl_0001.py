'''==================
  　ファイル分割・UTF-8変換処理
=================='''
import gzip
import os
import sys
import re
import boto3
from awsglue.utils import getResolvedOptions
from dtm_glue_job_lib.dl_dcl_0016 import log_start, log_end, log_info, log_error, log_warn

def conv_utf8():
    """ファイル分割・UTF-8変換処理"""
    try:
        # 環境変数にテーブルIDを指定
        os.environ['PG_ID'] = 'dl_dcl_0001'

        # 初期処理
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME', 'SRC_BUCKET_NAME', 'SRC_FOLDER_NAME',
            'DEST_BUCKET_NAME', 'CHUNK_SIZE','SRC_FILE_ENCODING','DELETE_FLG'
        ])
        log_info(f'ジョブパラメータ: {args}')

        # 処理開始日時情報出力
        start_time = log_start()

        # S3リソース取得
        s3_resource = boto3.resource('s3')

        # S3取得元バケット取得
        my_bucket = s3_resource.Bucket(args['SRC_BUCKET_NAME'])

        # 改行コード
        end_line = '\n'.encode(args['SRC_FILE_ENCODING'])

        # チャンクサイズ設定
        chunk_size = 1024 * 1024 * int(args['CHUNK_SIZE'])

        # データソース読み込み
        for obj in my_bucket.objects.filter(Prefix=args['SRC_FOLDER_NAME']):

            objkey_splitted = (obj.key).split('/')

            if not objkey_splitted[-1]:
                # フォルダの場合、次のオブジェクトを処理する
                continue

            if objkey_splitted[-1][-4:] != '.csv':
                # csvファイルでない場合、ログを出力する
                log_warn('CSV以外のファイルを検知しました：' + 'S3://' + args['SRC_BUCKET_NAME'] + '/' + obj.key)
                continue

            log_info(f'変換処理開始 :{obj.key}')

            # Streaming Body取得
            s_body = obj.get()['Body']

            # 保存用オブジェクトキーを生成
            target_key = obj.key

            # UTF-8変換の場合、「conv_utf8/」を除いた保存用オブジェクトキーを生成
            if objkey_splitted[0] == 'conv_utf8':
                target_key = '/'.join(objkey_splitted[1:])

            # 変数宣言
            # 最終行以降～チャンクサイズ分の末尾までのデータ
            partial_chunk = b''
            # 変換後データ
            result = ''
            # ヘッダ
            header = ''
            # ファイル数カウンタ
            cnt = 1
            # カラム数保持
            cols_count = 0

            # チャンクサイズで分割し、以下の処理を繰り返す
            while True:
                # チャンクサイズ分データを読み込み、前回の最終行以降のデータと結合
                # （一度readすると読込位置が移動し、次回read時には自動的に次の分が読まれる）
                chunk = partial_chunk + s_body.read(chunk_size)

                # 空の場合、ループを抜ける
                if chunk == b'':
                    break

                if cnt == 1:
                    # 1ファイル目の場合、ヘッダを保持
                    header = chunk[0:chunk.find(end_line) + 1].decode(args['SRC_FILE_ENCODING'])
                    # カラム数保持
                    cols_count = len(header.split(','))

                # 最終行の開始位置を取得
                last_newline = chunk.rfind(end_line)

                #改行コードがない場合エラー
                if last_newline < 0 :
                    log_error('分割指定サイズを超える行が存在します。')
                    raise Exception()

                # レコード終端判定を実施
                if len(chunk) >= chunk_size:
                    bef_start = last_newline
                    bef_end = last_newline
                    while True:
                        # 前行の終了位置
                        bef_end = bef_start
                        # 前行の開始位置
                        bef_start = chunk[:bef_end].rfind(end_line)
                        # 改行コードが見つからない場合（チャンクの先頭まで来た場合）、処理を抜ける
                        if bef_start == -1:
                            break
                        # 前行を取得
                        lastline_str = (
                            chunk[bef_start + 1:bef_end + 1].decode(args['SRC_FILE_ENCODING']))
                        # 取得した行の末尾がレコードの終端である場合、
                        # 終端のインデックスを保持し処理を抜ける
                        if (cols_count == len(lastline_str.split('\",\"')) and
                        re.match(r'....[^",]"\n|...[^",]"\r\n|.[^",]",""\n|[^",]",""\r\n'
                        , lastline_str[-7:])):

                            last_newline = bef_end
                            break
                        # 一致しない場合（最終レコードの途中の場合）、上記の処理を繰り返す

                # 最終レコード以前のデータをデコードし保持
                result = chunk[0:last_newline + 1].decode(args['SRC_FILE_ENCODING'])

                # 最終レコードのデータを保持
                partial_chunk = chunk[last_newline + 1:]

                if cnt != 1:
                    # 2ファイル目以降の場合、ヘッダを先頭に追加
                    result = header + result

                # UTF-8でエンコード
                result = result.encode('utf-8')

                # 書き込み
                s3_resource.meta.client.put_object(
                    Bucket=args['DEST_BUCKET_NAME'],
                    Body=gzip.compress(data=result, compresslevel=6),
                    Key=target_key.replace('.csv', '_' + str(cnt).zfill(2) + '.gz'))

                log_info('書き込み完了：' + 'S3://' + args['DEST_BUCKET_NAME'] + '/' +
                         target_key.replace('.csv', '_' + str(cnt).zfill(2) + '.gz'))

                # ファイル数をカウント
                cnt = cnt + 1

            # 削除
            if args['DELETE_FLG'] == '1':
                s3_resource.meta.client.delete_object(
                Bucket=args['SRC_BUCKET_NAME'], Key=obj.key)

        # 処理終了日時情報出力
        log_end(start_time)

    except Exception as ex:
        log_error('ERRORが発生しました。処理を終了します。\n' + str(ex))
        sys.exit(1)


# 実行
conv_utf8()
