<?php

use Aws\S3\S3Client;
use Aws\CommandPool;
use Aws\CommandInterface;
use Aws\ResultInterface;
use GuzzleHttp\Promise\PromiseInterface;
use Aws\AwsException;

use League\Flysystem\AdapterInterface;
use League\Flysystem\AwsS3v3\AwsS3Adapter;
use League\Flysystem\Filesystem;
$autoloader = require_once dirname(__FILE__) . DIRECTORY_SEPARATOR . 'vendor' . DIRECTORY_SEPARATOR . 'autoload.php';

if ( ! function_exists('write_log')) {
   function write_log ( $log )  {
      if ( is_array( $log ) || is_object( $log ) ) {
         error_log( print_r( $log, true ) );
      } else {
         error_log( $log );
      }
   }
}

/**
 * Creates settings page and sets default options
 */
$redis = null;

function dos_settings_page () {

  // Default settings
  if ( get_option('upload_path') == 'wp-content' . DIRECTORY_SEPARATOR . 'uploads' || get_option('upload_path') == null  ) {
    update_option('upload_path', WP_CONTENT_DIR . DIRECTORY_SEPARATOR . 'uploads');
  }

  if ( get_option('dos_endpoint') == null ) {
    update_option('dos_endpoint', 'https://ams3.digitaloceanspaces.com');
  }

  if ( get_option('dos_filter') == null ) {
    update_option('dos_filter', '');
  }

  if ( get_option('dos_storage_path') == null ) {
    update_option('dos_storage_path', '/');
  }

  if ( get_option('dos_redis_host') == null ) {
    update_option('dos_redis_host', '127.0.0.1');
  }

  if ( get_option('dos_redis_port') == null ) {
    update_option('dos_redis_port', '6379');
  }
	
  include_once('code/settings_page.php');

}

/**
 * Adds menu item for plugin
 */
function dos_create_menu (){

  add_options_page(
    'DigitalOcean Spaces Sync',
    'DigitalOcean Spaces Sync',
    'manage_options',
    __FILE__,
    'dos_settings_page'
  );

}

/**
 * Creates storage instance and returns it
 * 
 * @param  boolean $test
 * @return instance
 */
function __DOS ($test = false) {

  if ( $test ) {

    // dos_key
    if ( isset( $_POST['dos_key'] ) ) {
      $dos_key = $_POST['dos_key'];
    } else { 
      $dos_key = get_option('dos_key');
    }

    // dos_secret
    if ( isset( $_POST['dos_secret'] ) ) {
      $dos_secret = $_POST['dos_secret'];
    } else {
      $dos_secret = get_option('dos_secret');
    }

    // dos_endpoint
    if ( isset( $_POST['dos_endpoint'] ) ) {
      $dos_endpoint = $_POST['dos_endpoint'];
    } else {
      $dos_endpoint = get_option('dos_endpoint');
    }

    // dos_container
    if ( isset( $_POST['dos_container'] ) ) {
      $dos_container = $_POST['dos_container'];
    } else {
      $dos_container = get_option('dos_container');
    }

  } else {
    $dos_key = get_option('dos_key');
    $dos_secret = get_option('dos_secret');
    $dos_endpoint = get_option('dos_endpoint');
    $dos_container = get_option('dos_container');
  }

  $client = S3Client::factory([
    'credentials' => [
      'key'    => $dos_key,
      'secret' => $dos_secret,
    ],
    'endpoint' => $dos_endpoint,
    'region' => '',
    'version' => 'latest',
  ]);

  $connection = new AwsS3Adapter($client, $dos_container);
  $filesystem = new Filesystem($connection);

  return $filesystem;

}

/**
 * Displays formatted message
 *
 * @param string $message
 * @param bool $errormsg = false
 */
function dos_show_message ($message, $errormsg = false) {

  if ($errormsg) {

    echo '<div id="message" class="error">';

  } else {

    echo '<div id="message" class="updated fade">';

  }

  echo "<p><strong>$message</strong></p></div>";

}

/**
 * Tests connection to container
 */
function dos_test_connection () {

  try {
    
    $filesystem = __DOS( true );
    $filesystem->write('test.txt', 'test');
    $filesystem->delete('test.txt');
    dos_show_message(__('Connection is successfully established. Save the settings.', 'dos'));

    exit();

  } catch (Exception $e) {

    dos_show_message( __('Connection is not established.','dos') . ' : ' . $e->getMessage() . ($e->getCode() == 0 ? '' : ' - ' . $e->getCode() ), true);
    exit();

  }

}

/**
 * Trims an absolute path to relative
 *
 * @param string $file Full url path. Example /var/www/example.com/wm-content/uploads/2015/05/simple.jpg
 * @return string Short path. Example 2015/05/simple.jpg
 */
function dos_filepath ($file) {

  $dir = get_option('upload_path');
  $file = str_replace($dir, '', $file);
  $file = get_option('dos_storage_path') . $file;
  $file = str_replace('\\', '/', $file);
  $file = str_replace('//', '/', $file);
  $file = str_replace(' ', '%20', $file);
  //$file = ltrim($file, '/');

  return $file;
}

/**
 * Returns data as a string
 *
 * @param mixed $data
 * @return string
 */
function dos_dump ($data) {

  ob_start();
  print_r($data);
  $content = ob_get_contents();
  ob_end_clean();

  return $content;

}

/**
 * Uploads a file to storage
 * 
 * @param  string *Full path to upload file
 * @param  int Number of attempts to upload the file
 * @param  bool *Delete the file from the server after unloading
 * @return bool Successful load returns true, false otherwise
 */
function dos_file_upload ($pathToFile, $attempt = 0, $del = false) {
	global $redis;
  // init cloud filesystem
  $filesystem = __DOS();
  $regex = get_option('dos_filter');

  // prepare regex
  if ( $regex == '*' ) {
    $regex = '';
  }

  if (get_option('dos_debug') == 1) {

    $log = new Katzgrau\KLogger\Logger(
      plugin_dir_path(__FILE__) . '/logs', Psr\Log\LogLevel::DEBUG,
      array('prefix' => __FUNCTION__ . '_' . time() . '_', 'extension' => 'log')
    );

    if ($attempt > 0) {
      $log->notice('Attempt # ' . $attempt);
    }

  }

  try {

    if ( get_option('dos_debug') == 1 and isset($log) ) {

      $log->info("Path to thumbnail: " . $pathToFile);

      if ( dos_check_for_sync($pathToFile) ) {

        $log->info('File ' . $pathToFile . ' will be uploaded.');

      } else {

        $log->info('File ' . $pathToFile . ' does not fit the mask.');

      }
    }

    // check if readable and regex matched
    if ( is_readable($pathToFile) && ($regex == '' || !preg_match( $regex, $pathToFile) )) {

      $filesystem->put( dos_filepath($pathToFile), file_get_contents($pathToFile), [
        'visibility' => AdapterInterface::VISIBILITY_PUBLIC
      ]);

      if (get_option('dos_storage_file_only') == 1) {
        dos_file_delete($pathToFile);
      }

      if (get_option('dos_debug') == 1 and isset($log)) {
        $log->info("Instance - OK");
        $log->info("Name ObJ: " . dos_filepath($pathToFile));
      }
      
    }

    return true;

  } catch (Exception $e) {
	write_log($e->getCode() . ' :: ' . $e->getMessage());
    if ( get_option('dos_debug') == 1 and isset($log) ) {
      $log->error($e->getCode() . ' :: ' . $e->getMessage());
    }

    if ( !get_option('dos_retry_count') ||  $attempt < get_option('dos_retry_count') ) {
		if (get_option('dos_use_redis_queue')) {
			dos_redis_queue_push($pathToFile, ++$attempt, $del, 60);
		} else {
	      wp_schedule_single_event(time() + 60, 'dos_schedule_upload', array($pathToFile, ++$attempt, $del));
		}
    }

    return false;

  }

}

/**
 * Deletes a file from local filesystem 
 * 
 * @param  string $file Absolute path to file
 * @param  integer $attempt Number of attempts to upload the file
 */
function dos_file_delete ($file, $attempt = 0) {

  if (file_exists($file)) {

    if (is_writable($file)) {

      if (get_option('dos_debug') == 1) {

        $log = new Katzgrau\KLogger\Logger(plugin_dir_path(__FILE__) . '/logs', Psr\Log\LogLevel::DEBUG,
          array('prefix' => __FUNCTION__ . '_', 'extension' => 'log'));

      }

      unlink($file);

      if (get_option('dos_debug') == 1 and isset($log)) {
        $log->info("File " . $file . ' deleted');
      }

    } elseif ($attempt < 3) {

      wp_schedule_single_event(time() + 10, 'dos_file_delete', array($file, ++$attempt));

    }

  }

}

/**
 * Upload files to storage
 *
 * @param int $postID Id upload file
 * @return bool
 */
function dos_storage_upload ($postID) {
	global $redis;
  if ( wp_attachment_is_image($postID) == false ) {

    $file = get_attached_file($postID);

    if ( get_option('dos_debug') == 1 ) {

      $log = new Katzgrau\KLogger\Logger(plugin_dir_path(__FILE__) . '/logs', Psr\Log\LogLevel::DEBUG,
        array('prefix' => __FUNCTION__ . '_', 'extension' => 'log'));
      $log->info('Starts unload file');
      $log->info('File path: ' . $file);
      //$log->info("MetaData: \n" . dos_dump($meta));

    }

    if ( get_option('dos_lazy_upload') == 1 ) {
 		if ( get_option('dos_use_redis_queue') == 1) {
			dos_redis_queue_push($file, 0, false);

// 			if ($redis == null) {
// 				$redis = new Redis(); 
// 			   $redis->connect(get_option('dos_redis_host'), get_option('dos_redis_port')); 
// 			}
// 			write_log('Pushed to Redis: ' . $filepath);
		
		   //store data in redis list 
// 		   $redisdata = $filepath . ',0,1';
// 		   $redis->lpush("dos_upload_queue", $redisdata); 
// 		   
		
		} else {
			  wp_schedule_single_event( time(), 'dos_schedule_upload', array($file));
// 			write_log('Scheduled by wp cron: ' . $filepath);
			
		}


    } else {

      dos_file_upload($file);

    }

  }

  return true;
  
}

/**
 * Deletes the file from storage
 * @param string $file Full path to file
 * @return string
 */
function dos_storage_delete ($file) {

  try {

    if (get_option('dos_debug') == 1) {
      $log = new Katzgrau\KLogger\Logger(plugin_dir_path(__FILE__) . '/logs', Psr\Log\LogLevel::DEBUG,
        array('prefix' => __FUNCTION__ . '_', 'extension' => 'log'));
    }

    $filesystem = __DOS();

    $filesystem->delete( dos_filepath($file) );
    dos_file_delete($file);

    if (get_option('dos_debug') == 1 and isset($log)) {
      $log->info("Delete file:\n" . $file);
    }

    return $file;

  } catch (Exception $e) {

    return $file;

  }

}

/**
 * Uploads thumbnails using data from $metadata and adds schedule processes
 * @param array $metadata
 * @return array Returns $metadata array without changes
 */
function dos_thumbnail_upload ($metadata) {
	global $redis;
  $paths = array();
  $upload_dir = wp_upload_dir();

  if (get_option('dos_debug') == 1) {

    $log = new Katzgrau\KLogger\Logger(plugin_dir_path(__FILE__) . '/logs', Psr\Log\LogLevel::DEBUG,
      array('prefix' => __FUNCTION__ . '_', 'extension' => 'log'));
    $log->debug("Metadata dump:\n" . dos_dump($metadata));

  }

  // collect original file path
  if ( isset($metadata['file']) ) {

    $path = $upload_dir['basedir'] . DIRECTORY_SEPARATOR . $metadata['file'];
    array_push($paths, $path);

    // set basepath for other sizes
    $file_info = pathinfo($path);
    $basepath = isset($file_info['extension'])
        ? str_replace($file_info['filename'] . "." . $file_info['extension'], "", $path)
        : $path;

  }

  // collect size files path
  if ( isset($metadata['sizes']) ) {

    foreach ( $metadata['sizes'] as $size ) {

      if ( isset($size['file']) ) {

        $path = $basepath . $size['file'];
        array_push($paths, $path);

      }

    }

  }

  // process paths
  foreach ($paths as $filepath) {
    
    if ( get_option('dos_lazy_upload') ) {

 		if ( get_option('dos_use_redis_queue') == 1) {
			dos_redis_queue_push($filepath, 0, true, 2);
// 			if ($redis == null) {
// 				$redis = new Redis(); 
// 			   $redis->connect(get_option('dos_redis_host'), get_option('dos_redis_port')); 
// 	// 			write_log('Pushed to Redis: ' . $filepath);
// 			}
// 		   //store data in redis list 
// 		   $redisdata = $filepath . ',0,1';
// 		   $redis->lpush("dos_upload_queue", $redisdata); 
		} else {
	      wp_schedule_single_event(time() + 2, 'dos_schedule_upload', array($filepath, 0, true));
// 			write_log('Scheduled by wp cron: ' . $filepath);
			
		}
		

      if (get_option('dos_debug') == 1 and isset($log)) {
        $log->info("Add schedule. File - " . $filepath);
      }

    } else {

      // upload file
      dos_file_upload($filepath, 0, true);

      // log data
      if ( get_option('dos_debug') ) {
        $log->info("Uploaded file - " . $filepath);
      }

    }

  }

  if ( get_option('dos_debug') == 1 and isset($log) ) {

    $log->debug("Schedules dump: " . dos_dump(_get_cron_array()));

  }

  return $metadata;

}

/**
 * Rewrites wp_attachment_url if file matches regex/filter
 * @param string $url
 * @return string $url
 */

/**
 * @param string $pattern
 * @param int $flags = 0
 *
 * @return array|false
 */
function dos_glob_recursive ($pattern, $flags = 0) {

  $files = glob($pattern, $flags);
  foreach (glob(dirname($pattern) . DIRECTORY_SEPARATOR . '*', GLOB_ONLYDIR | GLOB_NOSORT) as $dir) {
    $files = array_merge($files, dos_glob_recursive($dir . DIRECTORY_SEPARATOR . basename($pattern), $flags));
  }

  return $files;

}

/**
 * Faster search in an array with a large number of files
 * @param string $needle
 * @param array $haystack
 * @return bool
 */
function dos_in_array ($needle, $haystack) {

  $flipped_haystack = array_flip($haystack);
  if (isset($flipped_haystack[$needle])) {
    return true;
  }

  return false;

}

/**
 * Checks if the file falls under the mask specified in the settings.
 * @param string @path Full path to file
 * @return bool
 */
function dos_check_for_sync ($path) {

  get_option('dos_filter') != '' ?
    $mask = trim(get_option('dos_filter')) :
    $mask = '*';

  if (get_option('dos_debug') == 1) {

    $log = new Katzgrau\KLogger\Logger(plugin_dir_path(__FILE__) . '/logs', Psr\Log\LogLevel::DEBUG,
      array('prefix' => __FUNCTION__ . '_', 'extension' => 'log'));
    $log->info('File path: ' . $path);
    $log->info('Short path: ' . dos_filepath($path));
    $log->info('File mask: ' . $mask);

  }

  $dir = dirname($path);
  if (get_option('dos_debug') == 1 and isset($log)) {

    $log->info('Directory: ' . $dir);

  }

  $files = glob($dir . DIRECTORY_SEPARATOR . '{' . $mask . '}', GLOB_BRACE);
  if (get_option('dos_debug') == 1 and isset($log)) {
    $log->debug("Files dump (full name):\n" . dos_dump($files));
  }

  $count = count($files) - 1;
  for ($i = 0; $i <= $count; $i++) {
    $files[$i] = dos_filepath($files[$i]);
  }

  if (get_option('dos_debug') == 1 and isset($log)) {
    $log->debug("Files dump (full name):\n" . dos_dump($files));
  }

  //$result = in_array(dos_filepath($path), $files,true);
  $result = dos_in_array(dos_filepath($path), $files);
  if (get_option('dos_debug') == 1 and isset($log)) {
    $result ? $log->info('Path found in files') : $log->info('Path not found in files');
  }

  return $result;

}

if (get_option('dos_storage_file_only') == 1) {
	function get_random_string() {
        return bin2hex(openssl_random_pseudo_bytes(8));
    }
    function dos_unique_file_name($filename, $filename_raw) {
        $elements = explode('.', $filename_raw);
        $size = sizeof($elements);
        if ($size>1) {
            $elements[$size-2] = $elements[$size-2] . '-' . get_random_string();
        } else {
            $elements[0] = $elements[0] . '-' .  get_random_string();
        }

        return implode('.', $elements);
    }
	add_filter('sanitize_file_name', 'dos_unique_file_name', 10, 2);

}

if (get_option('dos_lazy_upload') == 1 && get_option('dos_use_redis_queue') == 1) {
	function dos_redis_queue_push($pathToFile, $attempt = 0, $del = false, $delay = 0) {
		global $redis;
		if ($redis ==null) {
			$redis = new Redis(); 
			$redis->connect(get_option('dos_redis_host'), get_option('dos_redis_port')); 
		}
		$new_entry = $pathToFile . ',' . $attempt . ',' . ($del?"1":"0");
		$redis->zadd('dos_delayed_queue', time() + $delay, $new_entry);
	}
	function dos_redis_queue_pop($limit = 25) {
		global $redis;
		if ($redis ==null) {
			$redis = new Redis(); 
			$redis->connect(get_option('dos_redis_host'), get_option('dos_redis_port')); 
		}
		$redis->watch("dos_delayed_queue");
// 		$results = $redis->zRangeByScore('dos_delayed_queue', 0, time(), array('limit' => array(0, 1)); /* array('val2') */

		$results = $redis->zRangeByScore('dos_delayed_queue', 0, time(), array('limit' => array(0, (int)$limit)));
		write_log('Upload ' .  sizeof($results) . ' files');
		$redis->multi();
		if ($results) {
			foreach ($results as $entry) {
				$redis->zrem('dos_delayed_queue', $entry);
			}
		}
		if ($redis->exec())
			return $results;
		return array();
		
// 		$redisdata = $redis->rpop("dos_upload_queue"); 
// 		if (!$redisdata)
// 			return false;
// 		$args = explode(',',$redisdata);
// 		return array($args[0],(int)$args[1],$args[2]);		
	}
	function __S3 () {


		$dos_key = get_option('dos_key');
		$dos_secret = get_option('dos_secret');
		$dos_endpoint = get_option('dos_endpoint');

	  $client = S3Client::factory([
		'credentials' => [
		  'key'    => $dos_key,
		  'secret' => $dos_secret,
		],
		'endpoint' => $dos_endpoint,
		'region' => '',
		'version' => 'latest',
	  ]);

	  return $client;

	}
	function dos_check_redis_and_upload() {
		global $s3Client;
		global $autoloader;
		$dos_container = get_option('dos_container');
		if ($s3Client==null) {
			$s3Client = __S3();
		}
		//Pop a batch from the queue
		
		if (get_option('dos_redis_queue_batch_size') >0) {
			$batchSize = (int)get_option('dos_redis_queue_batch_size');
		} else {
			$batchSize = 25;
		}
// 		$autoloader = require "./vendor/autoload.php";
// 		$loadresult = require_once dirname(__FILE__) . DIRECTORY_SEPARATOR . 'vendor' . DIRECTORY_SEPARATOR . 'autoload.php';

		if (!class_exists('Aws\CommandPool')) {
			write_log("Class CommandPool not exists, loading now");
			$autoloader->loadClass("Aws\CommandPool");
		}
// 		$loadresult = $autoloader->findFile("Aws\CommandPool");
// 		exit(1);
		
		$jobs = dos_redis_queue_pop($batchSize); 
		$batchNumber = 0;
		while(sizeof($jobs) > 0 && $batchNumber++ < 10) {
			// Now create multiple commands for batching the files to S3
			$commands = array();
			foreach ($jobs as $entry) {
				
				$args = explode(',',$entry);
				if ( is_readable($args[0])) {
					$s3key = ltrim(dos_filepath($args[0]),'/');
					$ext = pathinfo($args[0], PATHINFO_EXTENSION);
					$commands[] = $s3Client->getCommand('PutObject', array(
						'Bucket' => $dos_container,
						'Key'    => $s3key,
						'Body' => fopen ( $args[0], 'r' ),
						'ACL' => 'public-read',
						'ContentType' => "image/$ext"
					));
				}
			}

			// Create a pool and provide an optional array of configuration
			$pool = new CommandPool($s3Client, $commands, [
				// Only send $batchSize files at a time (this is set to 25 by default)
				'concurrency' => $batchSize,
				// Invoke this function before executing each command
				'before' => function (CommandInterface $cmd, $iterKey) {
// 					write_log( "About to send {$iterKey}: "
// 						. print_r($cmd->toArray(), true) . "\n");
				},
				// Invoke this function for each successful transfer
				'fulfilled' => function (
					ResultInterface $result,
					$iterKey,
					PromiseInterface $aggregatePromise
				) use ($jobs) {
					$args = explode(',',$jobs[$iterKey]);
					$pathToFile = $args[0];
// 					write_log("Completed {$iterKey}: {$result} {$pathToFile}");
					if (get_option('dos_storage_file_only') == 1) {
						dos_file_delete($pathToFile);
					}
				},
				// Invoke this function for each failed transfer
				'rejected' => function (
					AwsException $reason,
					$iterKey,
					PromiseInterface $aggregatePromise
				) use ($jobs) {
					$args = explode(',',$jobs[$iterKey]);
					$attempt = (int)$args[1];
					$pathToFile = $args[0];
					write_log("Failed to upload {$pathToFile}: {$reason}");
					$del = $args[2];
					if ( !get_option('dos_retry_count') ||  $attempt < get_option('dos_retry_count') ) {
						dos_redis_queue_push($pathToFile, ++$attempt, $del, 60);
					}
				},
			]);

			// Initiate the pool transfers
			$promise = $pool->promise();

			// Force the pool to complete synchronously
			$promise->wait();
// 						
			$jobs = dos_redis_queue_pop($batchSize); 
		}
	}
	function dos_add_cron_recurrence_interval( $schedules ) {
		$feed_interval = 30;
		$schedules['dos_scan_schedule'] = array(
			'interval'  => $feed_interval,
			'display'   => __( sprintf('Every %d Seconds', $feed_interval), 'textdomain' )
		);
		return $schedules;
	}
	
	add_filter( 'cron_schedules', 'dos_add_cron_recurrence_interval' );

	if( !wp_next_scheduled( 'dos_scan_redis_hook' ) ) {
			wp_schedule_event( time(), 'dos_scan_schedule', 'dos_scan_redis_hook' );
		} else {
	}

	add_action( 'dos_scan_redis_hook', 'dos_check_redis_and_upload');

}

/**
 * Includes
 */
include_once('code/styles.php');
include_once('code/scripts.php');
include_once('code/settings.php');
include_once('code/actions.php');
include_once('code/filters.php');