<?php

function dos_register_settings() {

  register_setting('dos_settings', 'dos_endpoint');
  register_setting('dos_settings', 'dos_container');
  register_setting('dos_settings', 'dos_secret');
  register_setting('dos_settings', 'dos_key');
  register_setting('dos_settings', 'upload_url_path');
  register_setting('dos_settings', 'dos_storage_path');
  register_setting('dos_settings', 'upload_path');
  register_setting('dos_settings', 'dos_storage_file_only');
  register_setting('dos_settings', 'dos_storage_file_delete');
  register_setting('dos_settings', 'dos_lazy_upload');
  register_setting('dos_settings', 'dos_filter');
  register_setting('dos_settings', 'dos_debug');
  register_setting('dos_settings', 'dos_lazy_upload');
  register_setting('dos_settings', 'dos_use_redis_queue');
  register_setting('dos_settings', 'dos_redis_host');
  register_setting('dos_settings', 'dos_redis_port');
  register_setting('dos_settings', 'dos_retry_count');
  register_setting('dos_settings', 'dos_redis_queue_batch_size');
}
