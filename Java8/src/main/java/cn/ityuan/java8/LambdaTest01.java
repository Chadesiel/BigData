package cn.ityuan.java8;


import org.junit.Test;

/**
 * @Classname LambdaTest01
 * @Description TODO
 * @Date 2021/10/21 22:00
 * @Created by liudan
 */
public class LambdaTest01 {

    @Test
    public void test(){

        Runnable runnable = new Runnable() {
            public void run() {
                System.out.println("hello world");
            }
        };

        runnable.run();
        System.out.println("========================");


        Runnable runnable1 = () -> System.out.println("hello world ");
        runnable1.run();



    }
}
