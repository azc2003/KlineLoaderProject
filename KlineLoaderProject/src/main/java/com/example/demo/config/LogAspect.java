package com.example.demo.config;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Aspect
@Component
public class LogAspect {

    @Around("execution(* com.example.demo.service.KlineAggregateService.get(..))")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        long start = System.currentTimeMillis();
        Object result = joinPoint.proceed(); // 执行原方法
        long duration = System.currentTimeMillis() - start;
        System.out.println("Execution time: " + duration + "ms");
        return result;
    }

//    /**
//     * @Before
//     * @After
//     * @AfterThrowing
//     * @AfterReturning
//     *
//     * @Around
//     */
//    @Before("execution(public * com.example.demo.*.*.*(..))")
//    public void before(JoinPoint point){
//        System.out.println("before--"+point.getSignature().getName() + " => " + Arrays.toString(point.getArgs()));
//    }
//    @After("execution(public * com.example.demo.*.*.*(..))")
//    public void after(JoinPoint point){
//        System.out.println("after--"+point.getSignature().getName() + " => " + Arrays.toString(point.getArgs()));
//    }
//    @AfterReturning("execution(public * com.example.demo.*.*.*(..))")
//    public void afterReturning(JoinPoint point){
//        System.out.println("afterReturning--"+point.getSignature().getName() + " => " + Arrays.toString(point.getArgs()));
//    }
//    @AfterThrowing(pointcut = "execution(public * com.example.demo.*.*.*(..))",throwing = "exception")
//    public void afterThrowing(JoinPoint point,Throwable exception){
//        System.out.println("afterThrowing--"+point.getSignature().getName() + " => " + Arrays.toString(point.getArgs()));
//        System.out.println("Throwing message " + exception.getMessage());
//    }
//
//
//
//
//    @Around("execution(public * com.example.demo.*.*.*(..))")
//    public Object around(ProceedingJoinPoint point) throws Throwable {
//        System.out.println("around--"+point.getSignature().getName() + " => " + Arrays.toString(point.getArgs()));
//        long x = System.currentTimeMillis();
//
//        var object = point.proceed();
//        System.out.println(System.currentTimeMillis() - x);
//
//        return object;
//    }
}
