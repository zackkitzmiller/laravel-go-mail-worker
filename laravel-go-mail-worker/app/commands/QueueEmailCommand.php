<?php

use Illuminate\Console\Command;

class QueueEmailCommand extends Command {

    protected $name = 'email:send';

    protected $description = 'Send email from laravel to beandstalk with golang.';

    public function __construct()
    {
        parent::__construct();
    }

    public function fire()
    {
        $message = array(
            'to' => 'zack@inrpce.com',
            'toname' => 'Zack Kitzmiller',
            'subject' => 'Hello From Go!',
            'body' => 'Through Beanstalkd'
        );
        Queue::push('', $message);
    }
}
