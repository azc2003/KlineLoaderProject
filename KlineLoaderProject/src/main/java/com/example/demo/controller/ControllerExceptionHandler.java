package com.example.demo.controller;

import com.example.demo.model.ex.InputException;
import jakarta.validation.ConstraintViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Global exception handler for controller layer.
 * Captures and formats common exception responses.
 */
@RestControllerAdvice
public class ControllerExceptionHandler {

    /**
     * Handles custom input-related exceptions.
     *
     * @param ex InputException
     * @return 400 Bad Request with error message
     */
    @ExceptionHandler(InputException.class)
    public ResponseEntity<String> handleInputException(InputException ex) {
        return new ResponseEntity(ex.getMessage(), HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles validation constraint violations (e.g. @NotBlank).
     *
     * @param ex ConstraintViolationException
     * @return 400 Bad Request with validation error message
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<String> handleConstraintViolation(ConstraintViolationException ex) {
        return new ResponseEntity(ex.getMessage(), HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles missing required request parameters (e.g. missing @RequestParam).
     *
     * @param ex MissingServletRequestParameterException
     * @return 400 Bad Request with message about the missing parameter
     */
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<String> handleMissingParam(MissingServletRequestParameterException ex) {
        return new ResponseEntity(ex.getMessage(), HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles illegal arguments passed to controller methods.
     *
     * @param ex IllegalArgumentException
     * @return 400 Bad Request with error message
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleIllegalArgument(IllegalArgumentException ex) {
        return new ResponseEntity(ex.getMessage(), HttpStatus.BAD_REQUEST);
    }
}
