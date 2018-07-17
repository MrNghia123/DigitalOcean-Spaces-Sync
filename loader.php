<?php
/**
 * Plugin Name: Team Cao DigitalOcean Spaces Sync
 * Plugin URI: https://github.com/MrNghia123/DigitalOcean-Spaces-Sync
 * Description: This WordPress plugin syncs your media library with DigitalOcean Spaces Container. Developed by keeross(https://github.com/keeross). Enhanced by Nghia Huynh (https://github.com/MrNghia123).
 * Version: 1.1.0.3
 * Author: Nghia Huynh
 * Author URI: https://github.com/MrNghia123
 * License: MIT
 * Text Domain: dos
 * Domain Path: /languages

 */
load_plugin_textdomain('dos', false, dirname(plugin_basename(__FILE__)) . '/lang');

function dos_incompatibile($msg) {
  require_once ABSPATH . DIRECTORY_SEPARATOR . 'wp-admin' . DIRECTORY_SEPARATOR . 'includes' . DIRECTORY_SEPARATOR . 'plugin.php';
  deactivate_plugins(__FILE__);
  wp_die($msg);
}

if ( is_admin() && ( !defined('DOING_AJAX') || !DOING_AJAX ) ) {

  if ( version_compare(PHP_VERSION, '5.3.3', '<') ) {

    dos_incompatibile(
      __(
        'Plugin DigitalOcean Spaces Sync requires PHP 5.3.3 or higher. The plugin has now disabled itself.',
        'dos'
      )
    );

  } elseif ( !function_exists('curl_version')
    || !($curl = curl_version()) || empty($curl['version']) || empty($curl['features'])
    || version_compare($curl['version'], '7.16.2', '<')
  ) {

    dos_incompatibile(
      __('Plugin DigitalOcean Spaces Sync requires cURL 7.16.2+. The plugin has now disabled itself.', 'dos')
    );

  } elseif (!($curl['features'] & CURL_VERSION_SSL)) {

    dos_incompatibile(
      __(
        'Plugin DigitalOcean Spaces Sync requires that cURL is compiled with OpenSSL. The plugin has now disabled itself.',
        'dos'
      )
    );

  }

}
require_once dirname(__FILE__) . DIRECTORY_SEPARATOR . 'code.php';